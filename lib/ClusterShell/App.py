from flask import Flask, request
from flask_lambda import FlaskLambda

from ClusterShell.CLI.Clush import main as clush_main, format_nodes

# Import the necessary function from the core module

app = FlaskLambda(__name__)


# Define your Flask routes and endpoints
@app.route("/publish", methods=["POST"])
def publish():
    # expect comma separated hostnames
    environment = "dev"  # part of CF later
    data = request.json
    command = data.get("command")
    devices = format_nodes(data.get("devices", []))
    execute = clush_main(command, devices, environment)
    return "Welcome to the Flask app!"


@app.route("/results")
def retrieve_response():
    # result = quab()  # Call the function from the core module
    # return f"The result is: {result}"
    pass


# Add more routes and endpoints as needed

if __name__ == "__main__":
    app.run(debug=True)

    # This block will be executed when running `app.py` directly as a command-line tool
    # result = main()  # Call the function from the core module
    # print(f"The result is: {result}")
else:
    # This block will be executed when running `app.py` as a Flask app
    # You can configure additional Flask settings here if needed
    app.run(debug=True)
