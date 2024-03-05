# Pull base image
FROM postgres:16.2

# Set the working directory
WORKDIR /usr/src/app

# Set base credentials
ENV POSTGRES_USER postgres
ENV POSTGRES_HOST_AUTH_METHOD trust
ENV POSTGRES_DB aact

# Expose the port
EXPOSE 5432