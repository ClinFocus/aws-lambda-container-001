# Use the official Python 3.11 image as the base
FROM public.ecr.aws/lambda/python:3.11

# Set the working directory
WORKDIR /var/task

# Copy your Lambda function code to the container
COPY app.py ${LAMBDA_TASK_ROOT}

# Install additional libraries using pip
RUN pip install pandas mysql-connector-python sqlalchemy numpy

# Set the CMD to the name of your handler function
CMD [ "app.lambda_handler" ]
