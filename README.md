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

Cutting files into segments (based on newline) assumes that 

 * line order is not important,
 * newlines can be decoded from reading the file backwards - this is true for popular character encodings like [UTF-8], ASCII and ISO-8859-1. 
 * CSV entries are without linebreaks; not even linebreak wrapped in quotes.
 
If cutting into segments is not possible then using a [parallel reader](src/main/java/com/github/skjolber/unzip/csv/ParallelReader.java) is also an option; effectively decoding to characters and parsing in parallel.

Bugs, feature suggestions and help requests can be filed with the [issue-tracker].

## Obtain
The project is implemented in Java and built using [Maven]. The project is available on the central Maven repository.

Example dependency config:

```xml
<dependency>
    <groupId>com.github.skjolber.unzip-csv</groupId>
    <artifactId>unzip-csv</artifactId>
    <version>1.0.10</version>
</dependency>
```

# Usage
The top level `FileEntryHandler` is passed to the `ZipFileEngine`. 


```java
FileEntryHandler handler = ...; // your code here

ZipFileEngine engine = new ZipFileEngine(handler);
boolean success = engine.handle(new FileZipFileFactory(file));
```
where the default thread count is one per core.

## Pre- and post-processing
Wire `FileEntryHandler` methods 

```java
void beginFileCollection(String name);
void beginFileEntry(String name);
void endFileEntry(String name, ThreadPoolExecutor executor);
void endFileCollection(String name, ThreadPoolExecutor executor);
FileEntryStreamHandler getFileEntryStreamHandler(String name, long size, ThreadPoolExecutor executor);
```

for pre- or post-processing. Call the super method wherever it exists. Notice the `ThreadPoolExecutor` which allows for queuing more work.


## Sesseltjonna CSV parser
Implement an instance of `CsvLineHandlerFactory` to return specific handlers for each file in the ZIP archive.

Then use the provided adapters to implement a `FileEntryHandler` like in [this example](src/test/java/com/github/skjolber/unzip/TestSesselTjonnaCsvFileEntryHandler.java). 

## Univocity CSV parser
Implement an instance of `CsvLineHandlerFactory` to return specific handlers for each file in the ZIP archive. Then create a `DefaultUnivocityCsvFileEntryHandler` and pass it to the ZipFileEngine`.

# Details
The main performance-enhanching functions are
 * parallelization; unzip and process files and/or parts of files
 * unzip remote files while downloading only the necessary parts

ZIP files carry a `central directory` in the end of the file, detailing the name and location of the files within. parallel unzipping works better if the archive is compressed using the DEFLATE compression algorithm - see Apache [commons-compress](https://commons.apache.org/proper/commons-compress/zip.html) for additional details.


## Benchmarks
For simple [GTFS feeds] with an archive size of approximately 70 MB, which both unzips and processes file segments in parallel (parsing the CSV file lines), the performance on my laptop (4 cores + hyperthreading) is appoximately 1.5x-2x that of a linear unzip. 

Depending on your scenario, the the effect of processing file segments in parallel might be a considerable speedup, improving on the above result.

# Contact
If you have any questions or comments, please email me at thomas.skjolberg@gmail.com.

Feel free to connect with me on [LinkedIn], see also my [Github page].
## License
[Apache 2.0]

## Links
Other high-performance CSV parsers:

 * [SimpleFlatMapper](https://simpleflatmapper.org/)
 * [Univocity-parsers](https://github.com/uniVocity/univocity-parsers)

# History
 - 1.0.10: Maintenance release 
 - 1.0.9: Update CSV parsers
 - 1.0.8: Automatic module name for JDK9+.
 - 1.0.7: Add optional Parallel reader (from SimpleFlatMapper project).

[GTFS feeds]:			https://www.entur.org/dev/rutedata/
[Apache 2.0]: 			http://www.apache.org/licenses/LICENSE-2.0.html
[issue-tracker]:		https://github.com/skjolber/unzip-csv/issues
[Maven]:				http://maven.apache.org/
[LinkedIn]:				http://lnkd.in/r7PWDz
[Github page]:			https://skjolber.github.io
[UTF-8]:				https://stackoverflow.com/questions/22257486/iterate-backwards-through-a-utf8-multibyte-string
