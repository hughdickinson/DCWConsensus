<?php

class LineBoxMatcher{
  public $lines = null;
  public $boxes = null;
  public $subject = 0;

  function __construct($subject, $lines, $boxes){
    $this->lines = $lines;
    $this->boxes = $boxes;
    $this->subject = $subject;
  }

  function getLinesForBox($box){
    $minY = $box['meanY'];
    $maxY = $box['meanY'] + $box['meanHeight'];
    $minX = $box['meanX'];
    $maxX = $box['meanX'] + $box['meanWidth'];
    foreach ($this->lines as $line) {
      $meanLineY = 0.5*($line['meanY1'] + $line['meanY2']);

      $xOverlap = min($maxX, $line['meanX2']) - max($minX, $line['meanX1']);
      $xOverlapFraction = ($line['meanX2'] - $line['meanX1']) / $xOverlap;

      $lineIsInBox = $meanLineY > $minY && $meanLineY < $maxY;
      $lineIsInBox &= $xOverlapFraction > 0.95;//$line['meanX1'] > $minX && $line['meanX2'] < $maxX;

      if($lineIsInBox){
        $boxLines[] = $line;
      }
    }
    return $boxLines;
  }

  function computeBoxStats($boxLines){
    $boxStats['reliability'] = 0.0;
    foreach ($boxLines as $lineData) {
      $boxStats['reliability'] += $lineData['lineReliability'];
    }
    $boxStats['numLines'] = count($boxLines);
    $boxStats['reliability'] /= $boxStats['numLines'];
    return $boxStats;
  }

  public function getLinesForBoxes(){
    foreach ($this->boxes as $box) {
      $boxLines[$box['bestBoxIndex']] = $this->getLinesForBox($box);
    }

    foreach ($boxLines as $index => $boxLines) {
      $boxStats[$index] = $this->computeBoxStats($boxLines);
    }

    return array('boxLines' => $boxLines, 'boxStats' => $boxStats);
  }
}

function updateResults(&$lineResults, $lastRow, $lineWords){
  $lineResults[] = array(
    'id' => $lastRow['id'],
    'subjectId' => $lastRow['subjectId'],
    'bestLineIndex' => $lastRow['bestLineIndex'],
    'url' => $lastRow['url'],
    'meanX1'  => $lastRow['meanX1'],
    'meanX2' => $lastRow['meanX2'],
    'meanY1' => $lastRow['meanY1'],
    'meanY2' => $lastRow['meanY2'],
    'words' => $lineWords,
    'lineReliability' => $lastRow['lineReliability']
  );
}

$subject = $_GET['id'];

// load data from mysql database
$database = new mysqli('localhost', 'root', '!Ocus1!Ocus1', 'dcwConsensus');

if($database->connect_errno > 0){
  die('Unable to connect to database [' . $db->connect_error . ']');
}

$subjectQuery = "SELECT Subjects.url, Subjects.subjectReliability, Subjects.huntingtonId, SubjectLines.*, LineWords.* FROM Subjects LEFT JOIN (SubjectLines RIGHT JOIN LineWords ON LineWords.lineId = SubjectLines.id) ON (Subjects.id = SubjectLines.subjectId) WHERE Subjects.id = ".$subject." ORDER BY lineId, position, rank";

$subjectBoxQuery = "SELECT SubjectBoxes.* FROM SubjectBoxes WHERE SubjectBoxes.subjectId =".$subject;

$subjectTelegramQuery = "SELECT SubjectTelegrams.* FROM SubjectTelegrams WHERE SubjectTelegrams.subjectId =".$subject." ORDER BY SubjectTelegrams.telegramId";

if(!$subjectResult = $database->query($subjectQuery)){
  die('There was an error running the subject query [' . $db->error . ']');
}

if(!$subjectBoxResult = $database->query($subjectBoxQuery)){
  die('There was an error running the subject box query [' . $db->error . ']');
}

if(!$subjectTelegramResult = $database->query($subjectTelegramQuery)){
  die('There was an error running the subject telegram query [' . $db->error . ']');
}

// process textual transcription data
$lineWords = array();
$lineIndex = 0;
$lastRow = null;
$numRowsProcessed = 0;

while($row = $subjectResult->fetch_assoc()){
  // if a new line (following the first) has started
  if($row['bestLineIndex'] != $lineIndex){
    /* initialize and populate a new element in the results structure
    * to represent the line.
    */
    updateResults($lineResults, $lastRow, $lineWords);

    $lineIndex = $row['bestLineIndex'];
    $lineWords = array();

  }
  $lastRow = $row;
  $lineWords[$row['position']][$row['rank']] = $row['wordText'];

}
// append the data for the final line to the results set
updateResults($lineResults, $lastRow, $lineWords);

$subjectResult->free();

// process telegram boxing data
while($row = $subjectBoxResult->fetch_assoc()){
  $boxResults[] = $row;
}

$subjectBoxResult->free();

// process telegram number data
while($row = $subjectTelegramResult->fetch_assoc()){
  $telegramResults[] = $row;
}


$lineBoxMatcher = new LineBoxMatcher($subject, $lineResults, $boxResults);
$boxLineData = $lineBoxMatcher->getLinesForBoxes();

$telegramIndex=0;
foreach ($boxResults as &$boxResult) {
  if($boxResult['numBoxesMarked'] > 1){
    $boxResult['telegramData'] = $telegramResults[$telegramIndex++];
  }
  else{
    $boxResult['telegramData'] = null;
  }
}


$allresults = array('subjectData' => array('url' => $lastRow['url'], 'huntingtonId' => $lastRow['huntingtonId'], 'reliability' => $lastRow['subjectReliability']), 'telegramData' => $telegramResults, 'boxData' => $boxResults,'lineData' => $lineResults, 'boxLineData' => $boxLineData);

echo json_encode($allresults);
?>
