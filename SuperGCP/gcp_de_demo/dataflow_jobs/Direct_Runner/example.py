import apache_beam as beam # it is importing the beam module

def run_direct_runner_pipeline():
    with beam.Pipeline(runner='DirectRunner') as p:
        (p
         | 'Create' >> beam.Create(['student1@example.com', 'student2@example.com'])
         | 'Multiply by 2' >> beam.Map(lambda x: x.upper()) # Converting all the list elements into Uppercase
         | 'Print' >> beam.Map(print))

if __name__ == "__main__":       # it checks for the starting point of the python programme execution
    run_direct_runner_pipeline()
