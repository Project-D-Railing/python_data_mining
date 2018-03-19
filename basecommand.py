import abc


class Basecommand(object):
    @abc.abstractmethod
    def printHelp(self):
        raise NotImplementedError('printHelp needs to be implemented')

    @abc.abstractmethod
    def execute(self, scanner):
        raise NotImplementedError('printHelp needs to be implemented')