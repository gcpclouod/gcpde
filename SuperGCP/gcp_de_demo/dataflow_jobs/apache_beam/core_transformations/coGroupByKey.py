import apache_beam as beam

def run_pipeline():
    with beam.Pipeline() as p:
        # Example data for dataset A (names)
        names = (
            p | 'CreateNames' >> beam.Create([
                ('user1', 'Alice'),
                ('user2', 'Bob'),
                ('user3', 'Charlie')
            ])
        )

        # Example data for dataset B (scores)
        scores = (
            p | 'CreateScores' >> beam.Create([
                ('user1', 90),
                ('user2', 85)
            ])
        )

        # CoGroupByKey to join datasets
        joined_data = (
            {'names': names, 'scores': scores}
            | 'CoGroupByKey' >> beam.CoGroupByKey()
            | 'FormatOutput' >> beam.Map(lambda kv: (
                kv[0],
                {
                    'name': kv[1]['names'][0] if kv[1]['names'] else None,
                    'score': kv[1]['scores'][0] if kv[1]['scores'] else None
                }
            ))
            | 'Print' >> beam.Map(print)
        )

if __name__ == '__main__':
    run_pipeline()
