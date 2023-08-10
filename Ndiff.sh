#!/bin/bash

usage() { # use stderr and exit status != 0
    echo "Usage: $0 [options...] -D <dirA> -D <dirB>" >&2
    echo "-d             Show detailed differences, if any"
    echo "-l <limit>     Max differences shown per file (defaults to 3)"
    exit 2
}

set -o pipefail
# set -euo pipefail   # this is what can be called bash strict mode - it will make bash exit with an error
                    # if any command return non-zero, even in pipe or if a variable is undefined

OPTIND=1 # very IMPORTANT for getops + OPTARG to work at all
DETAIL=false
# handle command line options -h(elp) -d(etail) -l(limit) and -D(irs)
while getopts "hdl:D:" opt ; do
    case $opt in
        d ) DETAIL=true
            ;;
        l ) if [[ $OPTARG =~ ^[0-9]+$ ]]
            then
                LIMIT="$OPTARG"
            else
                echo "Warning: invalid limit \"$OPTARG\", defaulting to 3" 
                LIMIT=3
            fi
            ;;
        D ) dirs+=("$OPTARG")
            ;;
        * )
            usage
            exit
    esac
done

if [ ${#dirs[@]} -lt 2 ]
then
    usage
    exit
fi

for f in 0 1 ; do
    if [ ! -d ${dirs[$f]} ] ; then 
        echo "Directory does not exist: ${dirs[$f]}"
        exit
    fi
done

dirA=${dirs[0]}
dirB=${dirs[1]}

diff -q $dirA $dirB
error=$? # exit code needs to be captured here => tells us if any diff's were found (to be used below)

if [ $DETAIL = false ]
then
    exit
fi

# reaching here means -d used - Detail then, shiny colours ensue in the form of Escape Sequences

if [ $(git rev-parse --is-inside-work-tree &> /dev/null) ] ; then # we found a bug whereby git diff would not work under
    echo "Sorry, the detail option requires $0 outside the git work tree"
    exit
fi

RED='\033[0;31m'
GREEN='\033[0;32m'
WHITE='\033[1m'
NC='\033[0m' # Color Reset

# number of output lines we want from each detail diff, default: 3
if [ -z $LIMIT ] ; then LIMIT=3 ; fi

LIMCMD=" head -$(( $LIMIT + 2 ))" #(increase by 2), want 2 lines, make it 4 because of \n and header

if [ $error -ne 0 ]
then
    printf "${WHITE}\nDiff detail limited to ${LIMIT} lines per comparison: ${NC}${RED}red${NC}${WHITE} is diff on the left file, ${GREEN}green${NC}${WHITE} is for the right:${NC}"
    for f in `ls $dirA`
    do
        /usr/bin/git diff  -U0 --word-diff=color $dirA/$f $dirB/$f | \
            grep -v "\[1mindex" | grep -v "\[36m" | grep -v "+++" | grep -v "\-\-\-" | \
            sed -e "s/diff --git a\//\ndiff /" | sed -e "s/Ncsv b\//Ncsv /" | $LIMCMD
    done
fi