class StatefulWord():

    def __init__(self, _word, _span, _tagStates, _sentence):
        self.word = _word
        self.tagStates = _tagStates
        self.sentence = _sentence
        self.span = _span

    def __hash__(self):
        return self.word.__hash__()

    def __eq__(self, other):
        return self.word == other.word

    def __str__(self):
        return 'SW:' + str(self.word) + ' ' + str(self.tagStates)

    def __repr__(self):
        return 'SW:' + str(self.word)

    def asTuple(self):
        return (self.word, self.span, self.tagStates, self.sentence)
