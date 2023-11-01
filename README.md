
# Lambda Function for Data Processing

This repository contains the source code for an AWS Lambda function designed to process data from a specific CSV file and store the results in an RDS database. The function includes data transformations using libraries like Pandas, SQLAlchemy, NumPy, and DateTime. It's intended to be used in AWS Lambda with a custom runtime.

## Prerequisites

Before deploying this Lambda function, you need to ensure you have the following prerequisites set up:

1. An AWS account with the required IAM roles and permissions.
2. An S3 bucket where your CSV files are uploaded.
3. An RDS database for storing the processed data.

## Getting Started

1. Clone this repository to your local machine or your development environment.
2. Customize the following configuration variables in your Lambda function code:
   * `db_username`: Your RDS database username.
   * `db_password`: Your RDS database password.
   * `db_host`: The hostname of your RDS database.
   * `db_name`: The name of your RDS database.
   * `db_port`: The port for your RDS database. (Change it if necessary)
3. Customize the `specific_file_name` variable to specify the CSV file you want to trigger the Lambda for.
4. Ensure your S3 bucket is set up and configured to trigger this Lambda function when new files are uploaded.
5. Deploy the Lambda function to AWS and configure the event source (S3 bucket) to trigger the function.

## Docker Container

The repository includes a Dockerfile that you can use to create a custom runtime for your Lambda function. The Dockerfile is set up with Python 3.11 and includes the required libraries: Pandas, MySQL Connector (for SQLAlchemy), SQLAlchemy, NumPy, and DateTime libraries.

To build the Docker container, follow these steps:

1. Build the Docker container:
   <pre><div class="bg-black rounded-md"><div class="flex items-center relative text-gray-200 bg-gray-800 gizmo:dark:bg-token-surface-primary px-4 py-2 text-xs font-sans justify-between rounded-t-md"><span>shell</span><button class="flex ml-auto gizmo:ml-0 gap-2 items-center"><svg stroke="currentColor" fill="none" stroke-width="2" viewBox="0 0 24 24" stroke-linecap="round" stroke-linejoin="round" class="icon-sm" height="1em" width="1em" xmlns="http://www.w3.org/2000/svg"><path d="M16 4h2a2 2 0 0 1 2 2v14a2 2 0 0 1-2 2H6a2 2 0 0 1-2-2V6a2 2 0 0 1 2-2h2"></path><rect x="8" y="2" width="8" height="4" rx="1" ry="1"></rect></svg>Copy code</button></div><div class="p-4 overflow-y-auto"><code class="!whitespace-pre hljs language-shell">docker build -t my-lambda-function .
   </code></div></div></pre>
2. Push the container to your container registry (e.g., Amazon ECR):
   <pre><div class="bg-black rounded-md"><div class="flex items-center relative text-gray-200 bg-gray-800 gizmo:dark:bg-token-surface-primary px-4 py-2 text-xs font-sans justify-between rounded-t-md"><span>shell</span><button class="flex ml-auto gizmo:ml-0 gap-2 items-center"><svg stroke="currentColor" fill="none" stroke-width="2" viewBox="0 0 24 24" stroke-linecap="round" stroke-linejoin="round" class="icon-sm" height="1em" width="1em" xmlns="http://www.w3.org/2000/svg"><path d="M16 4h2a2 2 0 0 1 2 2v14a2 2 0 0 1-2 2H6a2 2 0 0 1-2-2V6a2 2 0 0 1 2-2h2"></path><rect x="8" y="2" width="8" height="4" rx="1" ry="1"></rect></svg>Copy code</button></div><div class="p-4 overflow-y-auto"><code class="!whitespace-pre hljs language-shell">docker tag my-lambda-function:latest <your-repo-uri>/my-lambda-function:latest
   docker push <your-repo-uri>/my-lambda-function:latest
   </code></div></div></pre>
3. Configure your AWS Lambda function to use the custom runtime by specifying the image URI.

## Usage

Once you have deployed the Lambda function and set up the Docker container, it will be automatically triggered by the specified S3 bucket when new files are uploaded. The function will process the data, perform data transformations, and store the results in your RDS database.

You can monitor the function's execution and logs in the AWS Lambda console.

## License

This code is provided under the MIT License. You are free to use, modify, and distribute the code as needed.

## Acknowledgments

This Lambda function is based on a specific use case for data processing and database integration. Feel free to adapt and extend it to meet your own requirements.
