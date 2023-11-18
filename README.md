# External Sort using Seastar

## Compile and build

To compile and build the application, run the following from the root of the repo :
```
cmake -B build -G Ninja .
cd build && ninja
```
or to use make
```
cmake -B build .
cd build && make -j <jobs>
```
### Seastar prerequisite

[Seastar](https://github.com/scylladb/seastar) framework is a dependency for building the `external-sort` app.
You can either build seastar locally yourself and pass the build directory when building external sort via `SEASTAR_BUILD_DIR` argument.
For example : 
```
cmake -B build -G Ninja -DSEASTAR_BUILD_DIR=/tmp/seastar/build/release .
cd build && ninja
```
(or) you can skip this argument and cmake will automatically pull the latest seastar code and build `external-sort` using that.

## Run external sort
Once built, it is pretty straighforward to run the `external-sort` app. In addition to the default command line arguments provided by the `seastar` framewrok, `external-sort` provides the following command line arguments :
```
App options:
  -h [ --help ]                    show help message
  --help-seastar                   show help message about seastar options
  --help-loggers                   print a list of logger names and exit
  -f [ --input-filename ] arg      Path to the file that has the records.
  -t [ --tempdir ] arg (="")       Path to the temp directory to store 
                                   intermediate files.
  -o [ --output-dir ] arg (="")    Directory to store the sorted result file. 
                                   By default, the result will be stored in the
                                   same directory as the input data.
  -v [ --verify-results ] arg (=0) Verify the external sort result

```
Only the `--input-filename` argument is required as the app needs to know where the records that needs to be sorted are stored.

To run the app and then verify the results :
```
./external-sort --input-filename /path/to/unsorted/records --verify-results=1
```

You can also use it in combination with the seastar arguments. For example to restrict the app to run only one 3 cores and with 200M memory :
```
./external-sort --input-filename /path/to/unsorted/records -c 3 -m 200M
```
