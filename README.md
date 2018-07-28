[![Build Status](https://travis-ci.org/skjolber/unzip-csv.svg)](https://travis-ci.org/skjolber/unzip-csv)

# unzip-csv
High-performance (i.e. multi-threaded) unpacking and processing of CSV files directly from ZIP archives.

Projects using this library will benefit from:
 * parallel unzip and processing of files (in streaming fashion)
 * per-file adapters (based on file-name)
 * hooks for pre- and post-processing 
 * cutting large files into segments for further parallel processing 
 * unzip specific files over-the-wire (with HTTP Range)
   * parallel, on-demand download
   * extract specific files without downloading the full archive

Cutting files into segments (based on newline) assumes that line order is not important.
 
Bugs, feature suggestions and help requests can be filed with the [issue-tracker].

## Obtain
The project is implemented in Java and built using [Maven]. The project is available on the central Maven repository.

Example dependency config:

```xml
<dependency>
    <groupId>com.github.skjolber.unzip-csv</groupId>
    <artifactId>core</artifactId>
    <version>1.0.0-SNAPSHOT</version>
</dependency>
```

# Usage

Implement an instance of `CsvLineHandler` and extend `AbstractCsvFileEntryHandler` to return specific handlers for each file in the ZIP archive. Then 

```java
MyCsvFileEntryHandler handler = ...; // your code here

ZipFileEngine engine = new ZipFileEngine(handler);
boolean success = engine.handle(new FileZipFileFactory(file));
```

where the default thread count is one per core. Optionally wrap your handler in `NewLineSplitterEntryHandler` for splitting (and processing) files into parts based on file-size and newlines.

### AbstractCsvFileEntryHandler
Implement the abstract method

```java
protected CsvLineHandler getHandler(String fileName, ThreadPoolExecutor executor) {
```

to return a specific implementation of `CsvLineHandler` for each file name. 

### CsvLineHandler
This is a simple interface for handling lines:
```java
void handleLine(Map<String, String> fields);
```

# Details
The main performance-enhanching functions are
 * parallelization; unzip and process files and/or parts of files
 * unzip remote files while downloading only the necessary parts

ZIP files carry a `central directory` in the end of the file, detailing the name and location of the files within. parallel unzipping works better if the archive is compressed using the DEFLATE compression algorithm - see Apache [commons-compress](https://commons.apache.org/proper/commons-compress/zip.html) for additional details.

## Pre- and post-processing
Additionally override `AbstractCsvFileEntryHandler` methods 

```java
void beginFileCollection(String name);
void beginFileEntry(String name);
void endFileEntry(String name, ThreadPoolExecutor executor);
void endFileCollection(String name, ThreadPoolExecutor executor);
```

for pre- or post-processing. Call the super method wherever it exists. Notice the `ThreadPoolExecutor` which allows for queueing more work.
## Benchmarks
For simple [GTFS feeds] with an archive size of approximately 70 MB, which both unzips and processes file segments in parallel (parsing the CSV file lines), the performance on my laptop (4 cores + hyperthreading) is appoximately 1.5x-2x that of a linear unzip. 

Depending on your scenario, the the effect of processing file segments in parallel might be a considerable speedup, improving on the above result.

# Contact
If you have any questions or comments, please email me at thomas.skjolberg@gmail.com.

Feel free to connect with me on [LinkedIn], see also my [Github page].
## License
[Apache 2.0]

# History
 - [1.0.0]: Initial release.

[GTFS feeds]:			https://www.entur.org/dev/rutedata/
[Apache 2.0]: 			http://www.apache.org/licenses/LICENSE-2.0.html
[issue-tracker]:		https://github.com/skjolber/unzip-csv/issues
[Maven]:			    http://maven.apache.org/
[LinkedIn]:			    http://lnkd.in/r7PWDz
[Github page]:			https://skjolber.github.io
[1.0.0]:		    	https://github.com/skjolber/unzip-csv/releases
