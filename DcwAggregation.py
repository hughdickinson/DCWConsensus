
import json
import csv
import pandas as pd
import numpy as np
import matplotlib.pyplot as mplplot
import dateutil.parser
import pickle
import sys
import re as regex
import copy
import itertools
import functools
import gc
import os
import glob
from collections import Counter, OrderedDict

# Classes to encapsulate single lines of text and multiple lines of text
from TextLine import TextLine
from TelegramLines import TelegramLines
from LineMatcher import LineMatcher
from MetaTagState import MetaTagState
from StatefulWord import StatefulWord

verbose = True
extraVerbose = False
if verbose:
    import pprint
    # from IPython.core.display import display
    pprinter = pprint.PrettyPrinter(indent=4)

# get_ipython().magic('matplotlib inline')

# ## Globally relevant variables

saveIdentifiedLineDetails = True  # Need to do this to recompute for every codebook
applyDoubleLineFix = False
applyDoubleWordFilter = True
applyDoubleLineFilter = True
classificationBaseDirectory = '/Users/hughdickinson/Google Drive/classifications'
consensusBaseDirectory = '/Users/hughdickinson/Google Drive/consensus/testing'
databaseNamePattern = 'dcwConsensus_{mss_label}'
aggregatedDataFileNamePattern = 'decoding-the-civil-war-aggregated_{mss_label}.txt'
# 'decoding-the-civil-war-consensus-linewise.csv'
aggregatedDataCsvFileNamePattern = 'decoding-the-civil-war-consensus-linewise_{mss_label}.csv'
# 'decoding-the-civil-war-consensus-subjectwise.csv'
aggregatedDataSubjectWiseCsvFileNamePattern = 'decoding-the-civil-war-consensus-subjectwise_{mss_label}_withBreaks.csv'
# 'dataWithLineIDs_subset.pkl'
identifiedLineFilePathPattern = 'dataWithLineIDs_subset_{mss_label}.pkl'
liveDate = dateutil.parser.parse("2016-06-20T00:00:00.00Z")


def loadSubjectData(subjectDataFileName):
    subject_data = []
    subjectColumns = ['subject_id', 'huntington_id', 'url']
    with open(subjectDataFileName) as csvfile:
        parsedSubjectCsv = csv.DictReader(csvfile)
        numPrinted = 0
        for subject in parsedSubjectCsv:
            parsedLocations = json.loads(subject['locations'])
            parsedMetaData = json.loads(subject['metadata'])
            if 'hdl_id' not in parsedMetaData:
                continue
            subject_data.append({
                'subject_id': int(subject['subject_id']),
                'huntington_id': parsedMetaData['hdl_id'],
                'url': parsedLocations['0']
            })
    subjectsFrame = pd.DataFrame.from_records(subject_data, index='subject_id')
    return subjectsFrame


# Parse the downloaded classification data into data structures for processing


def loadTelegrams(sampleDataFileName):

    telegrams = {}

    with open(sampleDataFileName) as csvfile:
        parsedCsv = csv.DictReader(csvfile)
        nTelegramsParsed = 0
        for recordIndex, record in enumerate(parsedCsv):
            done = False
            recordIsTelegram = True

            # check the date that the classification was made
            if "metadata" in record:
                parsedMetadata = json.loads(record["metadata"])
                parsedDate = dateutil.parser.parse(
                    parsedMetadata['started_at'])
                # skip "testing" data before the site went live
                if parsedDate < liveDate:
                    continue

            # parse the annotations and the subject data
            parsedAnnotations = json.loads(record["annotations"])
            parsedSubjectData = json.loads(record["subject_data"])

            # initialize container for transcribed lines
            transcribedLines = TelegramLines()

            # loop over tasks in the annotation
            for task in parsedAnnotations:
                # Check if the current record is for a telegram (tasks may be stored out of order, so
                # some tasks may be processed before non-telegrams are caught -
                # inefficient but unavoidable?)
                if task['task'] == "T1" and (
                        task['value'] is None
                        or not task['value'].startswith("Telegram")):
                    recordIsTelegram = False
                    break

                # Process transcriptions of text lines
                if task['task'].startswith("T12") and len(task['value']) > 0:
                    # process the lines that were transcribed for this task
                    for taskValueItem in task['value']:
                        transcribedLine = TextLine(
                            taskValueItem['x1'], taskValueItem['y1'],
                            taskValueItem['x2'], taskValueItem['y2'],
                            taskValueItem['details'][0]['value'])
                        transcribedLines.addLine(transcribedLine)

            # if the transcribed lines of a telegram have been processed then update the
            # list of independent transcriptions for this subject
            if recordIsTelegram:
                nTelegramsParsed += 1
                if int(record['subject_ids']) in telegrams:
                    telegrams[int(record['subject_ids'])].append(
                        (recordIndex, transcribedLines))
                else:
                    telegrams.update({
                        int(record['subject_ids']): [(recordIndex,
                                                      transcribedLines)]
                    })

    return telegrams, nTelegramsParsed

# Cast parsed data into structures that enable "straightfoward"
# aggregation analysis


def processLoadedTelegrams(telegrams):

    transcriptionLineStats = {}
    transcriptionLineDetails = []
    # loop over distinct subjects (currently individual telegram-type pages,
    # codebook handling to be implemented)
    for key, transcriptions in telegrams.items():
        totalLines = 0
        maxLines = 0
        minLines = sys.maxsize
        # loop over individual transcriptions of the subject
        for transcriptionData in transcriptions:
            transcription = transcriptionData[1]
            transcriptionIndex = transcriptionData[0]
            # process overall transcription statistics for this subject
            numLines = transcription.getNumLines()
            totalLines += numLines
            maxLines = numLines if numLines > maxLines else maxLines
            minLines = numLines if numLines < minLines else minLines
            # process the lines of the individual transcriptions of a subject
            for textLine in transcription.getLines():
                # Add a dictionary describing the current line
                lineDescription = {
                    'subjectKey': key,
                    'transcriptionIndex': transcriptionIndex,
                    'numLines': numLines,
                    'x1': textLine.getStart()['x'],
                    'y1': textLine.getStart()['y'],
                    'x2': textLine.getEnd()['x'],
                    'y2': textLine.getEnd()['y'],
                    'words': textLine.getWords()
                }
                transcriptionLineDetails.append(lineDescription)
        transcriptionLineStats.update({
            key: {
                'minLines': minLines,
                'maxLines': maxLines,
                'meanLines': totalLines / float(len(transcriptions))
            }
        })

    transcriptionLineDetailsFrame = pd.DataFrame(data=transcriptionLineDetails)
    transcriptionLineDetailsIndex = pd.MultiIndex.from_arrays([
        transcriptionLineDetailsFrame['subjectKey'],
        transcriptionLineDetailsFrame[
            'y1'], transcriptionLineDetailsFrame['y2'],
        transcriptionLineDetailsFrame[
            'x1'], transcriptionLineDetailsFrame['x2']
    ])
    transcriptionLineDetailsFrame = transcriptionLineDetailsFrame.set_index(
        transcriptionLineDetailsIndex)
    transcriptionLineDetailsFrame = transcriptionLineDetailsFrame.sort_index(
        level=0, sort_remaining=True)

    return transcriptionLineStats, transcriptionLineDetailsFrame


# Group transcriptions of individual lines according to spatial proximity
# NOTE: that a very important parameter for this process is the pixel
# tolerance that specifies the allowed disparity between corresponding *y*
# coordinates of separately marked annotated lines


def groupTranscriptionsLinewise(transcriptionLineDetailsFrame, lineTolerance=40, identifiedLineFilePath=None, saveIdentifiedLineDetails=False):

    if saveIdentifiedLineDetails or identifiedLineFilePath is None:
        transcriptionLineDetailsFrame['bestLineIndex'] = pd.Series(
            np.zeros_like(transcriptionLineDetailsFrame['subjectKey']),
            index=transcriptionLineDetailsFrame.index)
        # iterate over rows in sorted, grouped dataset and insert the best line
        # index
        bestLineIndex = -1
        currentSubject = -1
        numSubjectsProcessed = 0
        # line matcher with 40 pixel tolerance for y coordinates of lines that are
        # considered to be the same
        lineMatcher = LineMatcher(lineTolerance)
        for index, row in transcriptionLineDetailsFrame.iterrows():
            # if this is a new subject, reset the line index
            if currentSubject != index[0]:
                bestLineIndex = -1
                currentSubject = index[0]
                numSubjectsProcessed += 1
                if numSubjectsProcessed % 100 == 0:
                    print('Processed {0} subjects...'.format(
                        numSubjectsProcessed))

            # if the line coordinates do not match within tolerance, then increment
            # the line index
            if lineMatcher.compare((index[3], index[1], index[4], index[2])):
                bestLineIndex += 1

            # update the dataframe with the best line index
            transcriptionLineDetailsFrame.ix[index,
                                             'bestLineIndex'] = bestLineIndex
    else:
        identifiedLineFile = open(identifiedLineFilePath, 'rb')
        transcriptionLineDetailsFrame = pickle.load(identifiedLineFile)
        identifiedLineFile.close()

    return transcriptionLineDetailsFrame

# display(transcriptionLineDetailsFrame)
# pprinter.pprint(transcriptionLineStats)

# subjectsFrame.loc[1960106, 'url'].iloc[3]

# Experimental: Evaluate first pass line consensus
# Combine adjacent line groups that have very close mean Y values.


def doubleLineFix(transcriptionLineDetailsFrame, applyDoubleLineFix=False):
    if applyDoubleLineFix:
        transcriptionLineDetailsFirstPass = transcriptionLineDetailsFrame.set_index(
            'bestLineIndex', drop=False, append=True)
        transcriptionLineDetailsFirstPass[
            'oldBestLineIndex'] = transcriptionLineDetailsFirstPass[
                'bestLineIndex'].astype(np.int64)
        transcriptionLineDetailsFirstPass = transcriptionLineDetailsFirstPass.groupby(
            level=[0, 5]).aggregate({
                'y1': np.mean,
                'y2': np.mean,
                'bestLineIndex': 'first',
                'oldBestLineIndex': 'first'
            })
        transcriptionLineDetailsFirstPass['meanY'] = 0.5 * (
            transcriptionLineDetailsFirstPass['y1'] +
            transcriptionLineDetailsFirstPass['y2'])
        transcriptionLineDetailsFirstPass.set_index(
            'meanY', drop=False, append=True)

        oldColumnNames = transcriptionLineDetailsFirstPass.columns
        newColumnNames = [(name if name != 'bestLineIndex' else 'newBestLineIndex')
                          for name in oldColumnNames]
        transcriptionLineDetailsFirstPass.columns = newColumnNames

        thisRow = transcriptionLineDetailsFirstPass.iloc[0]
        # display(thisRow)

        meanYThreshold = 20
        bestLineIndexDecrement = 0

        for index, nextRow in transcriptionLineDetailsFirstPass.iterrows():
            if nextRow['newBestLineIndex'] == 0:  # new subject
                bestLineIndexDecrement = 0  # so reset the decrement
            elif np.abs(thisRow['meanY'] - nextRow['meanY']) < meanYThreshold:
                bestLineIndexDecrement += 1

            transcriptionLineDetailsFirstPass.loc[
                index, 'newBestLineIndex'] -= int(bestLineIndexDecrement)

            thisRow = nextRow

        transcriptionLineDetailsFirstPass.reset_index(
            level='bestLineIndex', drop=True, inplace=True)
        transcriptionLineDetailsFirstPass.columns = oldColumnNames

        transcriptionLineDetailsSecondPass = transcriptionLineDetailsFirstPass.set_index(
            'bestLineIndex', drop=False, append=True)
        transcriptionLineDetailsMismatches = transcriptionLineDetailsSecondPass[(
            transcriptionLineDetailsSecondPass['bestLineIndex'] !=
            transcriptionLineDetailsSecondPass['oldBestLineIndex'])]
        transcriptionLineDetailsMismatches.reset_index(
            level='bestLineIndex', drop=True, inplace=True)
        display(transcriptionLineDetailsMismatches)
        for mismatchIndex, mismatchRow in transcriptionLineDetailsMismatches.iterrows(
        ):
            for index, row in transcriptionLineDetailsFrame.loc[
                    mismatchIndex].iterrows():
                if row['bestLineIndex'] == mismatchRow['oldBestLineIndex']:
                    transcriptionLineDetailsFrame.loc[
                        mismatchIndex, 'bestLineIndex'] = mismatchRow[
                            'bestLineIndex']

    return transcriptionLineDetailsFrame


def computeConsensusWordReliability(wordOptions):
    uniqueWordOptions = list(set(wordOptions))
    # simple logic to return 0 reliability for words with very few consistent
    # transcriptions
    if len(wordOptions) < 2:
        return -0.25

    if (len(wordOptions) < 3 and len(uniqueWordOptions) > 1):
        return -0.5

    # more complicated logic that computes the fraction of transcribed words that equal the
    # consensus
    wordCounter = Counter(wordOptions)
    consensusWord, consensusWordCount = wordCounter.most_common(1)[0]
    return consensusWordCount / float(len(wordOptions))


# Now aggregate the text of the spatially matched lines
# In the process, identify, strip and note any metatags e.g.
# `[unclear][/unclear]` that surround individual words.

def aggregateSentences(sentences):
    metaTagState = MetaTagState()

    unclearPattern = r'(\[unclear\]).+?(\[/unclear\])'
    insertionPattern = r'(\[insertion\]).+?(\[/insertion\])'
    deletionPattern = r'(\[deletion\]).+?(\[/deletion\])'

    emptyTagPairPattern = r'\[([^/]+?)\]\[/\1\]'

    genericStartPattern = r'(\[([^/]+?)\])'
    genericEndPattern = r'(\[/(.+?)\])'

    aggregatedSentence = {
        'reliability': 0.0,
        'wordReliabilities': [],
        'words': []
    }
    statefulAggregatedSentence = {
        'reliability': 0.0,
        'wordReliabilities': [],
        'words': []
    }
    iSentence = 0
    for sentence in sentences:

        # metatag pairs are better described in "sentence coordinates", these
        # can always be mapped to words later
        fullSentence = ' '.join(sentence)

        # Remove any empty metatag pairs
        sentenceLength = len(sentence)
        while True:
            fullSentence = regex.sub(emptyTagPairPattern, '', fullSentence)
            if len(fullSentence) == sentenceLength:
                # no further replacement possible
                break
            else:
                sentenceLength = len(fullSentence)

        unclearResults = regex.finditer(unclearPattern, fullSentence)

        insertionResults = regex.finditer(insertionPattern, fullSentence)

        deletionResults = regex.finditer(deletionPattern, fullSentence)

        for unclearResult in unclearResults:
            '''print ('\nUnclear:\n', sentence, '\nnumgroups (start)', len(list(unclearResult.groups())), 'groups => ', list(unclearResult.groups()))
            for index, match in enumerate(unclearResult.groups()) :
                print (match, unclearResult.start(
                    index+1), unclearResult.end(index+1))
            print ('Tagging unclear between' , unclearResult.end(1), 'and', unclearResult.start(2))'''
            metaTagState.setTag('unclear',
                                unclearResult.end(1), unclearResult.start(2))

        for insertionResult in insertionResults:
            '''print ('\nInsertion:\n', sentence, '\nnumgroups (start)', len(list(insertionResult.groups())), 'groups => ', list(insertionResult.groups()))
            for index, match in enumerate(insertionResult.groups()) :
                print (match, insertionResult.start(index+1), insertionResult.end(index+1))'''
            metaTagState.setTag('insertion',
                                insertionResult.end(1),
                                insertionResult.start(2))

        for deletionResult in deletionResults:
            '''print ('\nDeletion:\n', sentence, '\nnumgroups (start)', len(list(deletionResult.groups())), 'groups => ', list(deletionResult.groups()))
            for index, match in enumerate(deletionResult.groups()) :
                print (match, deletionResult.start(index+1), deletionResult.end(index+1))'''
            metaTagState.setTag('deletion',
                                deletionResult.end(1), deletionResult.start(2))

        iWord = 0
        sentencePosition = 0
        for word in sentence:
            if (len(aggregatedSentence['words']) < iWord + 1):
                aggregatedSentence['words'].append([])
                statefulAggregatedSentence['words'].append([])

            nonMetaWord = regex.sub(genericStartPattern, '', word)
            nonMetaWord = regex.sub(genericEndPattern, '', nonMetaWord)
            if len(nonMetaWord) > 0:
                aggregatedSentence['words'][iWord].append(nonMetaWord)
                statefulAggregatedSentence['words'][iWord].append(
                    StatefulWord(nonMetaWord, (
                        sentencePosition, sentencePosition + len(word)
                    ), copy.deepcopy(metaTagState.getSetTags()), sentence))
                # Only increment wordcount if there was actually a word and not
                # just a collection of metatags
                iWord += 1
            sentencePosition += len(word) + 1

        metaTagState.reset()
        iSentence += 1
        # END LOOP OVER TRANSCRIBED SENTENCES ASSOCIATED WITH THIS LINE

    for wordOptions, statefulWordOptions in zip(
            aggregatedSentence['words'], statefulAggregatedSentence['words']):
        wordOptions.sort(key=Counter(wordOptions).get, reverse=True)
        statefulWordOptions.sort(
            key=Counter(statefulWordOptions).get, reverse=True)
        # Compute the reliability of each word's consensus transcription
        statefulAggregatedSentence['wordReliabilities'].append(
            computeConsensusWordReliability(wordOptions))
        # The word reliability can be outside the 0-1 range in special cases, so adjust clamp
        # those values appropriately
        clampedWordReliability = statefulAggregatedSentence[
            'wordReliabilities'][-1]
        clampedWordReliability = clampedWordReliability if clampedWordReliability >= 0.0 else 0.0
        clampedWordReliability = clampedWordReliability if clampedWordReliability <= 1.0 else 1.0

        statefulAggregatedSentence['reliability'] += clampedWordReliability
    try:
        statefulAggregatedSentence['reliability'] /= float(
            len(statefulAggregatedSentence['words']))
    except Exception as e:
        # print ('Exception on {} : {}'.format(sentence, e))
        statefulAggregatedSentence['reliability'] = 0.0

    return statefulAggregatedSentence


def processSentences(transcriptionLineDetailsFrame):

    # Several indices over the data were establshed to perform the
    # aggregation. hey are no longer required and a more informative index is
    # the index of the best matching line on the page.

    transcriptionLineDetailsReIndexed = transcriptionLineDetailsFrame.reset_index(
        level=[1, 2, 3, 4], drop=True)
    transcriptionLineDetailsReIndexed.set_index(
        'bestLineIndex', append=True, inplace=True)

    lineGroupedTranscriptionLineDetails = transcriptionLineDetailsReIndexed.groupby(
        level=[0, 1]).aggregate({
            "words":
            aggregateSentences,
            'subjectKey':
            lambda x: x.iloc[0],
            'y1':
            np.mean,
            'y2':
            np.mean,
            'x1':
            np.mean,
            'x2':
            np.mean,
            'transcriptionIndex':
            lambda x: tuple([xi for xi in x]),
            'numLines':
            lambda x: tuple([xi for xi in x])
        })

    lineGroupedTranscriptionLineDetails = lineGroupedTranscriptionLineDetails.reset_index(
        level=[1])
    lineGroupedTranscriptionLineDetails = pd.merge(
        lineGroupedTranscriptionLineDetails,
        subjectsFrame,
        left_index=True,
        right_index=True,
        how='left')

    return lineGroupedTranscriptionLineDetails

# ## Save the "most popular" transcriptions
# Also attempt to filter out double words e.g. `cheese cheese` and
# adjacent lines with a large fraction of shared text that are likely to
# erroneously repeated.


def saveAggregatedData(lineGroupedTranscriptionLineDetails, aggregatedDataCsvFileName, aggregatedDataSubjectWiseCsvFileName):

    aggregatedDataFile = open(aggregatedDataCsvFileName, 'w')
    aggregatedDataSubjectWiseFile = open(
        aggregatedDataSubjectWiseCsvFileName, 'w')
    # detailedAggregatedDataFile = open(aggregatedDataFileName, 'w')
    currentSubject = -1
    lastConsensusSentenceWords = (0, [])
    for index, row in lineGroupedTranscriptionLineDetails.iterrows():
        if index != currentSubject:
            if currentSubject != -1:
                aggregatedDataSubjectWiseFile.write('"\n')
            aggregatedDataSubjectWiseFile.write(
                '{0}@@{1}@@{2}@@"'.format(index, row['huntington_id'], row['url']))

            currentSubject = index

        # count the number of transcriptions that contributed to a sentence in case a deadlock between
        # duplicate lines must be broken - currently not used
        numTranscribedWords = functools.reduce(
            lambda total, increment: total + increment,
            [len(wordlist) for wordlist in row['words']['words']], 0)

        consensusSentenceWords = [
            wordlist[0].word for wordlist in row['words']['words']
            if len(wordlist) > 0
        ]
        consensusSentence = ' '.join(consensusSentenceWords)

        doubleWordRegex = r' ([^ ]{2,}) \1 ?'
        doubleWordMatch = regex.search(
            pattern=doubleWordRegex, string=consensusSentence)
        cleanConsensusSentence = consensusSentence
        if applyDoubleWordFilter and doubleWordMatch is not None:
            cleanConsensusSentence = regex.sub(
                pattern=doubleWordRegex,
                repl=lambda match: ' ' + match.group(1) + ' ',
                string=consensusSentence)
            if extraVerbose:
                print('Found double word {} in "{}" => {}'.format(
                    doubleWordMatch.group(1), consensusSentence,
                    cleanConsensusSentence))

        cleanConsensusWords = cleanConsensusSentence.split(' ')

        # Identify duplicate sentences after double word removal (currently only
        # exact duplicates)
        lineWordIntersection = [
            word for word in cleanConsensusWords
            if word in lastConsensusSentenceWords[1]
        ]
        if applyDoubleLineFilter and (len(lineWordIntersection) == max(
                len(cleanConsensusWords), len(lastConsensusSentenceWords[1]))):
            if extraVerbose:
                print('Found duplicate sentence "{}" == "{}" ({})'.format(
                    cleanConsensusSentence, ' '.join(lastConsensusSentenceWords[
                        1]), lineWordIntersection))
            # Do not write the duplicate sentence to file
            continue

        # only update if the current sentence was not a duplicate of the
        # previous.
        lastConsensusSentenceWords = (
            numTranscribedWords, consensusSentenceWords)

        aggregatedDataFile.write('{0}@@{1}@@{2}@@{3}@@{4}@@{5}\n'.format(
            currentSubject,
            row['huntington_id'],
            row['bestLineIndex'],
            '"' + cleanConsensusSentence + '"',
            #'(' + str(row['y1']) + ', ' + str(row['y2']),
            [len(wordlist) for wordlist in row['words']['words']],
            # row['numLines'],
            row['url']))
        # Note "<br />" line break sequence added at request of Huntington
        aggregatedDataSubjectWiseFile.write(
            '{0}<br />'.format(cleanConsensusSentence))
    aggregatedDataFile.close()
    aggregatedDataSubjectWiseFile.close()


if __name__ == '__main__':

    # Processing multiple classificaton files

    classificationCsvFiles = glob.glob(
        '{}/*.csv'.format(classificationBaseDirectory))

    # consensusCsvFiles = glob.glob('{}/*.csv'.format(consensusBaseDirectory))

    # consensusMssLabels = set([
    #     consensusCsvFile.split('/')[-1][len(
    #         'decoding-the-civil-war-consensus-subjectwise_'):
    #         -len('_withBreaks_clean.csv')]
    #     for consensusCsvFile in consensusCsvFiles
    #     if 'withBreaks' in consensusCsvFile
    # ])
    # classificationMssLabels = set([
    #     classificationCsvFile.split('/')[-1][len('classification_export_'):-4]
    #     for classificationCsvFile in classificationCsvFiles
    # ])
    #
    # remainingMssLabels = classificationMssLabels - consensusMssLabels
    #
    # remainingClassificationCsvFiles = list(
    #     sorted([
    #         '{base_dir}/classification_export_{label}.csv'.format(
    #             base_dir=classificationBaseDirectory, label=mssLabel)
    #         for mssLabel in remainingMssLabels
    #     ]))

    # print(*enumerate(remainingClassificationCsvFiles), sep='\n')

    # ledgerIndex = 30
    for sampleDataFileName in classificationCsvFiles:
        print('Processing {}...'.format(sampleDataFileName))
        # mssLabel = remainingClassificationCsvFiles[ledgerIndex].split('/')[-1][len(
        #     'classification_export_'):-4]
        mssLabel = sampleDataFileName.split('/')[-1][len(
            'classification_export_'):-4]
        # databaseName = databaseNamePattern.format(
        #     mss_label=mssLabel
        # )
        # 'dcwConsensusDoubleLineFix' if applyDoubleLineFix else 'dcwConsensus'
        # sampleDataFileName = classificationCsvFiles[
        #     ledgerIndex]  # 'decoding-the-civil-war-classifications-2.csv'
        aggregatedDataFileName = aggregatedDataFileNamePattern.format(
            mss_label=mssLabel)  # 'decoding-the-civil-war-aggregated.txt'
        aggregatedDataCsvFileName = aggregatedDataCsvFileNamePattern.format(
            mss_label=mssLabel)  # 'decoding-the-civil-war-consensus-linewise.csv'
        aggregatedDataSubjectWiseCsvFileName = aggregatedDataSubjectWiseCsvFileNamePattern.format(
            mss_label=mssLabel)  # 'decoding-the-civil-war-consensus-subjectwise.csv'
        identifiedLineFilePath = identifiedLineFilePathPattern.format(
            mss_label=mssLabel)  # 'dataWithLineIDs_subset.pkl'

        subjectDataFileName = 'decoding-the-civil-war-subjects-7-24-17.csv'

        subjectsFrame = loadSubjectData(subjectDataFileName)

        telegrams, nTelegramsParsed = loadTelegrams(sampleDataFileName)
        print('Parsed {} telegrams and stored {}.'.format(
            nTelegramsParsed, len(telegrams)))

        transcriptionLineStats, transcriptionLineDetailsFrame = processLoadedTelegrams(
            telegrams)

        transcriptionLineDetailsFrame = groupTranscriptionsLinewise(
            transcriptionLineDetailsFrame, 40, identifiedLineFilePath, saveIdentifiedLineDetails)

        # This is an intentional no-op
        transcriptionLineDetailsFrame = doubleLineFix(
            transcriptionLineDetailsFrame, applyDoubleLineFix=False)

        # The previous step is time consuming so serialize the processed data at
        # this stage
        if saveIdentifiedLineDetails:
            transcriptionLineDetailsFrame.to_pickle(identifiedLineFilePath)

        lineGroupedTranscriptionLineDetails = processSentences(
            transcriptionLineDetailsFrame)
        saveAggregatedData(lineGroupedTranscriptionLineDetails,
                           aggregatedDataCsvFileName, aggregatedDataSubjectWiseCsvFileName)


#
# # ## Plot reliability distributions
#
# # In[25]:
#
#
# gc.collect()
# allWordReliabilities = np.array(
#     list(
#         itertools.chain(* [
#             wordData['wordReliabilities']
#             for wordData in lineGroupedTranscriptionLineDetails["words"]
#         ])))
# mplplot.figure(figsize=(7, 7))
# mplplot.suptitle('Distribution of word reliabilities', fontsize=15)
# mplplot.xlabel('Word reliability')
# mplplot.ylabel('Number of words')
# allWordReliabilityAxis = mplplot.hist(
#     allWordReliabilities[allWordReliabilities > 0],
#     bins=20,
#     histtype='step',
#     alpha=0.2,
#     fill=True,
#     fc='r',
#     ec='r',
#     label=r"$R = N_{\rm con}/N_{\rm trans}$")
# allWordReliabilityAxis = mplplot.hist(
#     allWordReliabilities[np.logical_and(allWordReliabilities > -0.3,
#                                         allWordReliabilities < 0.0)],
#     bins=20,
#     histtype='step',
#     alpha=0.2,
#     fill=True,
#     fc='g',
#     ec='g',
#     label=r"$N_{\rm trans} < 3,\;R\rightarrow 0$")
# allWordReliabilityAxis = mplplot.hist(
#     allWordReliabilities[np.logical_and(allWordReliabilities > -0.6,
#                                         allWordReliabilities < -0.3)],
#     bins=20,
#     histtype='step',
#     alpha=0.2,
#     fill=True,
#     fc='b',
#     ec='b',
#     label=r"$N_{\rm trans} < 4 \wedge N_{\rm opt} > 1,\;R\rightarrow 0$")
# handles, labels = mplplot.gcf().gca().get_legend_handles_labels()
# mplplot.legend(handles[:], labels[:], loc='upper left')
# mplplot.show()
#
#
# # In[26]:
#
#
# gc.collect()
# allWordReliabilities = np.array(
#     list(
#         itertools.chain(* [
#             wordData['wordReliabilities']
#             for wordData in lineGroupedTranscriptionLineDetails["words"]
#         ])))
# mplplot.figure(figsize=(7, 7))
# mplplot.suptitle(
#     'Distribution of word non-edge case reliabilities < 1', fontsize=15)
# mplplot.xlabel('Word reliability')
# mplplot.ylabel('Number of words')
# allWordReliabilityAxis = mplplot.hist(
#     allWordReliabilities[np.logical_and(allWordReliabilities > 0,
#                                         allWordReliabilities < 1)],
#     bins=20,
#     histtype='step',
#     alpha=0.2,
#     fill=True,
#     fc='r',
#     ec='r',
#     label=r"$R = N_{\rm con}/N_{\rm trans}$")
# handles, labels = mplplot.gcf().gca().get_legend_handles_labels()
# mplplot.legend(handles[:], labels[:], loc='upper left')
# mplplot.show()
#
#
# # In[27]:
#
#
# gc.collect()
# allSentenceReliabilities = np.array([
#     wordData['reliability']
#     for wordData in lineGroupedTranscriptionLineDetails["words"]
# ])
# mplplot.figure(figsize=(7, 7))
# mplplot.suptitle('Distribution of sentence reliabilities', fontsize=15)
# mplplot.xlabel('Sentence reliability')
# mplplot.ylabel('Number of sentences')
# allSentenceReliabilityAxis = mplplot.hist(
#     allSentenceReliabilities,
#     bins=20,
#     histtype='step',
#     alpha=0.2,
#     fill=True,
#     fc='r',
#     ec='r')
#
#
# # In[28]:
#
#
# gc.collect()
# subjectGroupedTranscriptionLineDetails = lineGroupedTranscriptionLineDetails.groupby(level=0).aggregate(
#     {'words': lambda sentences: np.sum([sentence['reliability'] for sentence in sentences]) / float(len(sentences))})
# mplplot.figure(figsize=(7, 7))
# mplplot.suptitle('Distribution of subject reliabilities', fontsize=15)
# mplplot.xlabel('Subject reliability')
# mplplot.ylabel('Number of subjects')
# allSentenceReliabilityAxis = subjectGroupedTranscriptionLineDetails[
#     'words'].plot.hist(
#         bins=20, histtype='step', alpha=0.2, fill=True, fc='r', ec='r')
#
#
# # In[29]:
#
#
# from IPython.display import Audio
# Audio(
#     filename='/Users/hughdickinson/Downloads/jdk1.8.0_112/demo/applets/JumpingBox/sounds/danger.au',
#     autoplay=True)
#
#
# # In[30]:
#
#
# gc.collect()
# retiredSubjectGroupedTranscriptionLineDetails = lineGroupedTranscriptionLineDetails[lineGroupedTranscriptionLineDetails['retired']].groupby(
#     level=0).aggregate({'words': lambda sentences: np.sum([sentence['reliability'] for sentence in sentences]) / float(len(sentences))})
# mplplot.figure(figsize=(7, 7))
# mplplot.suptitle(
#     'Distribution of subject reliabilities for retired subjects', fontsize=15)
# mplplot.xlabel('Subject reliability')
# mplplot.ylabel('Number of subjects')
# allSentenceReliabilityAxis = retiredSubjectGroupedTranscriptionLineDetails[
#     'words'].plot.hist(
#         bins=20, histtype='step', alpha=0.2, fill=True, fc='r', ec='r')
#
#
# # ## Store consensus data in MySQL database
#
# # The consensus data for the telegrams are stored in a MySQL database that was created using the following commands:
# #
# # ```sql
# # CREATE DATABASE dcwConsensus;
# #
# # USE dcwConsensus;
# #
# # CREATE TABLE Subjects (
# # id INT AUTO_INCREMENT NOT NULL PRIMARY KEY,
# # zooniverseId INT NOT NULL,
# # huntingtonId CHAR(20) NOT NULL,
# # url VARCHAR(500) NOT NULL,
# # subjectReliability DECIMAL(5,4) NOT NULL DEFAULT 0.0
# # );
# #
# # CREATE TABLE SubjectLines (
# # id INT AUTO_INCREMENT NOT NULL PRIMARY KEY,
# # subjectId INT NOT NULL,
# # bestLineIndex INT NOT NULL,
# # meanX1 DECIMAL(7,3) NOT NULL,
# # meanX2 DECIMAL(7,3) NOT NULL,
# # meanY1 DECIMAL(7,3) NOT NULL,
# # meanY2 DECIMAL(7,3) NOT NULL,
# # lineReliability DECIMAL(5,4) NOT NULL DEFAULT 0.0
# # );
# #
# # CREATE TABLE LineWords (
# # id INT AUTO_INCREMENT NOT NULL PRIMARY KEY,
# # lineId INT NOT NULL,
# # wordText VARCHAR(100) CHARACTER SET utf16,
# # position INT NOT NULL,
# # rank INT NOT NULL,
# # transcriptionIndex INT NOT NULL,
# # spanStart INT NOT NULL,
# # spanEnd INT NOT NULL,
# # wordReliability DECIMAL(5,4) NOT NULL DEFAULT 0.0
# # );
# #
# # CREATE TABLE MetaTags (
# # id INT AUTO_INCREMENT NOT NULL PRIMARY KEY,
# # bestLineIndex INT NOT NULL,
# # transcriptionIndex INT NOT NULL,
# # state ENUM('unclear', 'insertion', 'deletion') NOT NULL,
# # start INT NOT NULL,
# # end INT NOT NULL,
# # UNIQUE KEY(bestLineIndex, transcriptionIndex, state, start)
# # );
# # ```
# sys.path.append('/Library/Python/2.7/site-packages')
# import mysql.connector
# #testSubjectData = lineGroupedTranscriptionLineDetails.iloc[0]
# '''connection = mysql.connector.connect(user=os.environ['DCW_MYSQL_USER'], password=os.environ['DCW_MYSQL_PASS'],
#                               host=os.environ['DCW_MYSQL_HOST'],
#                               database=databaseName)'''
# connection = mysql.connector.connect(
#     user='root',
#     password='!Ocus1!Ocus1',
#     host='localhost',
#     database=databaseName)
#
# cursor = connection.cursor()
# sentence = ''
# try:
#     subjectInsertQuery = (
#         "INSERT INTO Subjects "
#         "(zooniverseId, huntingtonId, url, subjectReliability) "
#         "VALUES (%s, %s, %s, %s)")
#
#     lineInsertQuery = (
#         "INSERT INTO SubjectLines "
#         "(subjectId, bestLineIndex, meanX1, meanX2, meanY1, meanY2, lineReliability) "
#         "VALUES (%s, %s, %s, %s, %s, %s, %s)")
#
#     wordInsertQuery = (
#         "INSERT INTO LineWords "
#         "(lineId, wordText, position, rank, transcriptionIndex, spanStart, spanEnd, wordReliability) "
#         "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)")
#
#     metaTagInsertQuery = (
#         "INSERT INTO MetaTags "
#         "(bestLineIndex, transcriptionIndex, state, start, end) "
#         "VALUES (%s, %s, %s, %s, %s)")
#
#     currentSubject = -1
#     subjectId = None
#
#     # Keep a record of the saved metatags
#     savedMetaData = {}
#
#     # Loop over aggregated lines in consensus data
#     for index, row in lineGroupedTranscriptionLineDetails.iterrows():
#         if index in subjectGroupedTranscriptionLineDetails.index.values:
#             if index != currentSubject:
#                 # If the subject has changed, insert a new subject entry
#                 subjectData = (int(index), row['huntington_id'], row['url'],
#                                float(subjectGroupedTranscriptionLineDetails.
#                                      loc[int(index)]['words']))
#                 cursor.execute(subjectInsertQuery, subjectData)
#                 subjectId = cursor.lastrowid
#                 currentSubject = index
#         else:
#             print('Subject index not found for: ', row)
#         # Insert the aggregated line data
#         bestLineIndex = int(row['bestLineIndex'])
#         lineData = (subjectId, int(bestLineIndex), row['x1'], row['x2'],
#                     row['y1'], row['y2'], row['words']['reliability'])
#         cursor.execute(lineInsertQuery, lineData)
#         lineId = cursor.lastrowid
#
#         # Loop over word positions in the aggregated line
#         for wordPosition, wordList in enumerate(row['words']['words']):
#             # Loop over words at each position
#             for wordRank, word in enumerate(wordList):
#                 sentence = word.sentence
#                 wordTranscriptionIndex = int(
#                     row['transcriptionIndex'][wordRank])
#                 wordData = (lineId, word.word[0:99], wordPosition, wordRank,
#                             int(wordTranscriptionIndex), word.span[0],
#                             word.span[1], 0.0)
#                 cursor.execute(wordInsertQuery, wordData)
#                 wordId = cursor.lastrowid
#                 # only insert data for each set of metatags once
#                 if len(word.tagStates) > 0 and (
#                         wordTranscriptionIndex not in savedMetaData
#                         or bestLineIndex not in
#                         savedMetaData[wordTranscriptionIndex]):
#                     if wordTranscriptionIndex in savedMetaData:
#                         savedMetaData[wordTranscriptionIndex].append(
#                             bestLineIndex)
#                     else:
#                         savedMetaData.update({
#                             wordTranscriptionIndex: [bestLineIndex]
#                         })
#                     for tag, spans in word.tagStates.items():
#                         for span in spans:
#                             metaTagData = (int(bestLineIndex),
#                                            int(wordTranscriptionIndex), tag,
#                                            span[0], span[1])
#                             cursor.execute(metaTagInsertQuery, metaTagData)
#
# except mysql.connector.Error as err:
#     print("Failed INSERT: {0}, {1}".format(sentence, err))
#
# connection.commit()
#
# cursor.close()
# connection.close()
# # In[ ]:
