### Explanation
```python
Explanation:
Predicate Function (is_odd):

The function is_odd checks whether a number is odd by using the modulo operator (%). If the number is odd (number % 2 != 0), it returns True, meaning the number will be included in the output PCollection.
Creating a PCollection:

The beam.Create transform is used to create a PCollection from a list of integers [1, 2, 3, 4, 5, 6, 7, 8, 9, 10].
Applying the beam.Filter Transform:

The beam.Filter transform applies the is_odd function to each element in the PCollection. Only the elements for which is_odd returns True are kept in the output PCollection.
Printing the Results:

The final filtered PCollection, which contains only odd numbers, is printed to the console.