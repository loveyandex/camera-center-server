# Use the official Golang image as the base image
FROM golang:1.23.4-alpine

# Set the working directory inside the container
WORKDIR /app

# Copy go mod and sum files
COPY go.mod go.sum ./

# Download dependencies
RUN go mod download

# Copy the source code
COPY main.go ./

# Build the application
RUN go build -o camera-api .

# Expose port 8080
EXPOSE 8080

# Run the binary
CMD ["./camera-api"]
