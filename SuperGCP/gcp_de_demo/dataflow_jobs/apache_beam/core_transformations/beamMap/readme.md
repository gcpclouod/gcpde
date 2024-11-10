# User-Defined Function (UDF)

This section explains the UDF `add_ten(x)` used in the pipeline.

## Function: `add_ten(x)`

`add_ten(x)` is a simple function that takes a number `x` and returns `x + 10`.

### Applying the First `beam.Map` Transform

The first `beam.Map` transform squares each number in the pipeline.

### Applying the Second `beam.Map` Transform

The second `beam.Map` transform applies the `add_ten` UDF to add 10 to each squared number.

### Printing the Results

Finally, the results after applying both transforms are printed to the console.

### Example Code
```python
def add_ten(x):
    return x + 10

# Applying first beam.Map to square the numbers
squared_numbers = input | beam.Map(lambda x: x**2)

# Applying second beam.Map to add 10 using the UDF
final_output = squared_numbers | beam.Map(add_ten)


### Key Steps:
1. Square the numbers using the first `beam.Map`.
2. Apply the `add_ten` UDF to the squared numbers.
3. Print the final results.

### Why Use Two beam.Map Transforms?

- **Step 1:** Square the input numbers to perform initial transformation.
- **Step 2:** Add 10 to the squared numbers using the UDF `add_ten` for further processing.
- This can be useful in data processing pipelines where multiple transformations are needed.

## Conclusion

By using two `beam.Map` transforms, you can chain operations and apply user-defined functions (UDFs) for custom transformations. This example showcases how to square numbers and then add 10 using the `add_ten` function in an Apache Beam pipeline.
