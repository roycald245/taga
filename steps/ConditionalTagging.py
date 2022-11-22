from steps.IStep import IStep


class ConditionalTagging(IStep):

    def __init__(self, model):
        self.model = model

    def process(self, df):
        for bdt_name, bdt_body in self.model.items():
            conditions = bdt_body.get('conditional_tagging', [])
            for condition in conditions:

