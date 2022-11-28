TEST_MODEL = {
    'tagging': {
        'fullest_name': [{
            'form': 'raw',
            'references': [
                {
                    'type': 'column',
                    'value': 'fullest_name1'
                },
                {
                    'type': 'column',
                    'value': 'fullest_name2'
                }
            ]
        }],
        'gugu_id': [{
            'form': 'raw',
            'references': [
                {
                    'type': 'constant',
                    'value': 'gugugu'
                }
            ]
        }],
        'imei': [{
            'form': 'raw',
            'references': [
                {'type': 'column', 'value': 'imei'}
            ]
        }],
        'id': [{
            'form': 'raw',
            'references': [
                {'type': 'column', 'value': 'id'}
            ]
        }],
        'pstn': [{
            'form': 'raw',
            'references': [
                {'type': 'column', 'value': 'pstn'}
            ]
        }]
    }
}
