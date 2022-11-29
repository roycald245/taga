TEST_MODEL = {
    'tagging': {
        'pstn': [{
            'form': 'raw',
            'references': [
                {
                    'type': 'column',
                    'value': 'phone'
                }
            ]
        }],
        'fullest_name': [{
            'form': 'raw',
            'references': [
                {
                    'type': 'column',
                    'value': 'fullest_name1'
                },
                {
                    'type': 'constant',
                    'value': 'WHAT'
                },
                {
                    'type': 'column',
                    'value': 'fullest_name2'
                }
            ]
        }],
        'zira': [{
            'form': 'raw',
            'references': [
                {
                    'type': 'constant',
                    'value': 'gugu'
                }
            ]
        }]
    }
    # },
    # 'conditions': [
    #     {
    #         'condition_column': 'pstn_type',
    #         'effected_column': 'pstn',
    #         'bdt_to_predicates_mapping': {'voip_number': ['voip'], 'personal_pstn': ['personal']}
    #     }
    # ]
}
