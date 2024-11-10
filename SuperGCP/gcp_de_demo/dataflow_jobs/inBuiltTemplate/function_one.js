var lineNumber = 0;

function transform(line) {
    lineNumber++;
    // Skip the first line (header)
    if (lineNumber === 1) {
        return null;
    }
    
    var values = line.split(',');
    var transformed = {
        'id': values[0],            // Updated column name
        'name': values[1],          // Updated column name
        'age': values[2],           // Updated column name
        'joining_date': values[3],  // Updated column name
        'joining_time': values[4],  // Updated column name
        'salary': values[5],        // Updated column name
        'is_active': values[6],     // Updated column name
    };
    return JSON.stringify(transformed);  // Convert the object to a JSON string
}
