import apache_beam as beam

def filter_on_age(element): # (1, raja, 20)
    villager_id, name, age, pension = element # tuple unpacking
    if int(age) >= 60:
        return [element]  # Return the element if age is 60 or above
    return []  # Return an empty list if age is below 60

def increment_pension(element):
    villager_id, name, age, pension = element
    return (villager_id, name, int(age), int(pension) + 1000)  # Increment the pension by 1000

with beam.Pipeline() as p:
    villagers = (
        p
        | 'ReadVillagers' >> beam.io.ReadFromText(r'C:\Users\UMESH\Downloads\gcp_de_demo\dataflow_jobs\texfile.csv', skip_header_lines=1)
        | 'ParseCSV' >> beam.Map(lambda line: line.split(','))
        | 'FilterOnAge' >> beam.FlatMap(filter_on_age)
        | 'IncrementPension' >> beam.Map(increment_pension)
        | 'PrintResults' >> beam.Map(print)
    )
