import functions_framework

@functions_framework.http
def square_number(request):
    """HTTP Cloud Function that returns the square of a given integer number."""
    print(request)
    
    # Check for 'number' in JSON body or query parameters
    request_json = request.get_json(silent=True)
    print(request_json)
    request_args = request.args
    print(request_args)

    # Try to get 'number' from JSON or query parameters
    if request_json and 'number' in request_json:
        number = request_json['number']
    elif request_args and 'number' in request_args:
        number = request_args['number']
    else:
        return "Please provide a valid integer number as 'number' parameter.", 400

    # Check if the number is an integer and calculate its square
    try:
        number = int(number)
        squared_value = number ** 2
        return f"The square of {number} is {squared_value}.", 200
    except ValueError:
        return "The 'number' parameter should be a valid integer.", 400
