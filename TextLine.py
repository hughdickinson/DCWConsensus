class TextLine():

    def __init__(self, x1, y1, x2, y2, text):
        self.coords = {'start': {'x': x1, 'y': y1}, 'end': {'x': x2, 'y': y2}}
        self.text = text
        self.words = text.split()
        self.numWords = len(self.words)

    def __str__(self):
        return str(
            self.text) + " @ ((" + str(self.coords['start']['x']) + ", " + str(
                self.coords['start']['y']) + "), (" + str(
                    self.coords['end']['x']) + ", " + str(
                        self.coords['end']['y']) + "))"

    def getStart(self):
        return self.coords['start']

    def getEnd(self):
        return self.coords['end']

    def getText(self):
        return str(self.text)

    def getCoords(self):
        return self.coords

    def getWords(self):
        return self.words
