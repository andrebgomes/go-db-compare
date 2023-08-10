# go-db-compare

go-db-compare is a tool that compares mysql databases schema and data.

The tool has four different strategies:

1. `dump`: creates Ncsv files inside the specified directory according to the specified database connection. Each Ncsv file corresponds to one database table.
2. `twodumps`: does the same thing as `dump` but for two database connections at the same time.
3. `live`: compares the schema and data of two database connections and stops at the first difference encountered (no output if no differences are found).
4. `diff`: compares two directories containing Ncsv's (previously created with `dump` or `twodumps`)


### Testing

`make test` will run the tests.

### Building

`make build` will compile the application and produce its binary in `bin/compare`.

### Running

`make help`:
```
./bin/compare -h
Usage of ./bin/compare:
  -c string
    	path to config file (e.g. config.yaml)
  -s string
    	strategy [dump, twodumps, live, diff]
```

E.g.:
`./bin/compare -c config23.yaml -s live`


### Notes

- ignored columns and types do not apply to strategy `diff`
- when running strategy `diff`, don't do it inside a git project folder, or results will come up empty

