import apache_beam as beam

def run_pipeline():
    with beam.Pipeline() as p:
        # Example dataset
        scores = (
            p | 'CreateScores' >> beam.Create([
                ('user1', 10),
                ('user2', 15),
                ('user1', 20),
                ('user3', 5),
                ('user2', 10)
            ])
        )

        # Group by key and sum the values
        aggregated_scores = (
            scores
            | 'GroupByKey' >> beam.GroupByKey()
            | 'SumScores' >> beam.Map(lambda kv: (kv[0], len(kv[1])))
            | 'Print' >> beam.Map(print)
        )

if __name__ == '__main__':
    run_pipeline()

''' 
GroupByKey and CoGroupByKey are operations used in distributed data processing frameworks like Apache Beam. 
They are used for different purposes but both involve grouping data by keys. Here’s a breakdown of each with examples:

1. GroupByKey
Purpose: Groups values by their keys within a single dataset. It’s often used to aggregate or process data associated with each key.

Example Scenario: You have a dataset of (key, value) pairs where each key corresponds to a user's ID and each value is a user's score. 
You want to calculate the total score for each user.



'''