from model import Model
from steps.IStep import IStep


class ConditionalTagging(IStep):

    def __init__(self, model: Model):
        self.model = model

    def process(self, df):
        cases = []
        for condition in self.model.conditions:
            cases.append()

