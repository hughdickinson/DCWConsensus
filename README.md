# DCWConsensus

## Synopsis:

Simple text consensus generation and user-friendly rendering on the *Decoding the Civil War* project.

## Operation:

The programmes in this repository are designed to reduce raw Zooniverse classification data in `.csv` format (which can be obtained by requesting a *new classification export* from the *Data Exports* facility provided by the *Panoptes* Project Builder. The reduced data are stored in a MySQL database from where they may be subsequently accessed and rendered via a web-based interface.

### Initial Reduction:
Initial reduction, aggregation and database storage is performed using Python 3 compatible code stored as jupyter notebooks. The Python code listed in `TestDcwAggregation.ipynb` parses the CSV, extracts data of interest and performs aggregation operations that are designed to derive a *consensus transcription* for every line that has been transcribed by Zooniverse volunteers. 

The aggregation process associates fragments of transcribed text based on the spatial proximity of graphical lines that the transcribers are instructed to draw beneath each line of text that they transcribe.

Each line of text is tokenized into words and groups of words that share the same position for spatially conicident lines are associated. The most frequently occuring word in that group is designated as the consensus transcription for the word at that position on that line.

The *reliability* of each **word**'s transcription is defined as the ratio of the number of transcriptions in the word group that agree with the consensus to the total number of transcriptions in the word group. **Line** reliabilities are defined as the ratio of the sum of the reliabilities of the words that comprise the line to the total number of words that comprise the line. **Subject** and **telegram** reliabilities are differently sized aggregates of lines and their reliabilities are similarly computed as the ratio of the sum of the reliabilities of their somponent lines to the total number of lines they comprise.

A single telegram page may contain all or part of several distinct telegrams. Accordingly, transcribers are also instructed to draw rectangular boxes around distinct telegrams that may constitute each subject. The Python 3 code listed in `TestTelegramBoxConsensus.ipynb` performs spatioal aggregation of the drawn boxes' coordinates to obtain a unified consensus of the position of distinct telegram fragments within a subject.

### Web-based rendering:
A humanly-accessible interface to the reduced consensus data is provided via the interaction of a JavaScript-driven client-side application (currently implemented as an embedded scrip element in `index.html`), which issues data requests to a remote server-side script `serveConsensus.php` implemented using PHP. The server-side script queries the `MySQL` database that was pre-populated by the Python aggregation code. It formats the data it retries using as JSON and transmits these data to the client where they are interpreted and rendered graphically via the web browser.

The client-side interface supports liited interactivity, which allows the user to probe details of the transcription and also provides a side-by-side view of the original subject image and the consensus text transcription.

