from mrjob.job import MRJob
import re

WORD_RE = re.compile(r"[\w']+")

class BigramCount(MRJob):

    def mapper(self, _, line):
        words = WORD_RE.findall(line.lower())
        for i in range(1, len(words)):
            bigram = f"{words[i-1]},{words[i]}"
            yield bigram, 1

    def reducer(self, bigram, counts):
        yield bigram, sum(counts)

if __name__ == '__main__':
    BigramCount.run()