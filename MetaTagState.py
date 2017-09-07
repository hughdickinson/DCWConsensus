class MetaTagState():

    def __init__(self):
        self.setTags = {}

    def setTag(self, tag, start, end):
        if tag in self.setTags:
            self.setTags[tag].append((start, end))
        else:
            self.setTags.update({tag: [(start, end)]})
        return self.setTags[tag]

    def reset(self):
        self.setTags = {}

    def getSetTags(self):
        return self.setTags
