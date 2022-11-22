from steps.AddConsts import AddConsts
from steps.ConcatBdt import ConcatBdt


def execute_workflow(df, model):
    df, model = ConcatBdt(model).process(df)
    df, model = AddConsts(model).process(df)
    return df, model
