from mrjob.job import MRJob
import re

# Define the list of stopwords
STOPWORDS = {"the", "and", "of", "a", "to", "in", "is", "it"}

WORD_RE = re.compile(r'\b\w+\b')

class NonStopWordCount(MRJob):

    def mapper(self, _, line):
        words = WORD_RE.findall(line.lower())
        for word in words:
            if word not in STOPWORDS:
                yield word, 1

    def reducer(self, word, counts):
        yield word, sum(counts)

if __name__ == '__main__':
    NonStopWordCount.run()