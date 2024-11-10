### Explanation:
```python
Creating a PCollection:

The beam.Create transform is used to create a PCollection of key-value pairs, where the key is a category (e.g., Electronics, Clothing, Books) and the value is an item name.
Applying the beam.GroupByKey Transform:

The beam.GroupByKey transform groups the values by their key. After this transform, each key in the resulting PCollection is associated with a list of values that share the same key.
Printing the Results:

The final PCollection contains key-value pairs where each key is associated with a list of items. The results are printed to the console.