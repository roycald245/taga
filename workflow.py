from steps.ConcatBdt import ConcatBdt


def execute_workflow(df, model):
    df, model = ConcatBdt(model).process(df)
    return df, model
