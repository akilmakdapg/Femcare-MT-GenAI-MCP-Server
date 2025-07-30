# Azure Cosmos DB MCP Server

A Model Context Protocol (MCP) server that provides seamless integration with Azure Cosmos DB. This server enables AI assistants and other MCP clients to interact with Cosmos DB databases through a standardized interface.

## Features

- **Document Operations**: Create, read, update, and delete documents
- **Query Execution**: Run SQL queries against Cosmos DB containers
- **Resource Discovery**: Browse database and container information
- **Statistics**: Get container statistics and metadata
- **Cross-partition Support**: Execute queries across multiple partitions
- **Async Operations**: Built with asyncio for high performance

## Prerequisites

- Python 3.8 or higher
- Azure Cosmos DB account and database
- Valid Cosmos DB connection credentials

## Installation

1. Clone this repository:
```bash
git clone <repository-url>
cd automatic_report_generation
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Set up environment variables:
```bash
cp .env.example .env
```

4. Edit `.env` file with your Cosmos DB credentials:
```env
COSMOS_ENDPOINT=https://your-account.documents.azure.com:443/
COSMOS_KEY=your-primary-key
COSMOS_DATABASE_NAME=your-database-name
COSMOS_CONTAINER_NAME=your-container-name
```

## Configuration

### Required Environment Variables

- `COSMOS_ENDPOINT`: Your Cosmos DB account endpoint URL
- `COSMOS_KEY`: Primary or secondary key for authentication
- `COSMOS_DATABASE_NAME`: Name of the database to connect to
- `COSMOS_CONTAINER_NAME`: Name of the container to operate on

### Optional Environment Variables

- `COSMOS_CONSISTENCY_LEVEL`: Consistency level (default: "Session")
- `COSMOS_CONNECTION_MODE`: Connection mode (default: "Gateway")

## Usage

### Running the Server

```bash
python main.py
```

The server will start and listen for MCP client connections via stdio.

### Available Tools

#### 1. Query Documents
Execute SQL queries against your Cosmos DB container:
```json
{
  "name": "query_documents",
  "arguments": {
    "query": "SELECT * FROM c WHERE c.category = 'electronics'",
    "cross_partition": true
  }
}
```

#### 2. Create Document
Create a new document:
```json
{
  "name": "create_document",
  "arguments": {
    "document": {
      "name": "Product Name",
      "category": "electronics",
      "price": 99.99
    }
  }
}
```

#### 3. Read Document
Read a specific document by ID:
```json
{
  "name": "read_document",
  "arguments": {
    "document_id": "document-id",
    "partition_key": "partition-key-value"
  }
}
```

#### 4. Update Document
Update an existing document:
```json
{
  "name": "update_document",
  "arguments": {
    "document_id": "document-id",
    "document": {
      "name": "Updated Product Name",
      "price": 89.99
    },
    "partition_key": "partition-key-value"
  }
}
```

#### 5. Delete Document
Delete a document:
```json
{
  "name": "delete_document",
  "arguments": {
    "document_id": "document-id",
    "partition_key": "partition-key-value"
  }
}
```

#### 6. Get Container Statistics
Get information about your container:
```json
{
  "name": "get_container_statistics",
  "arguments": {}
}
```

### Available Resources

- `cosmosdb://database`: Database information
- `cosmosdb://container`: Container information and schema
- `cosmosdb://documents`: Sample documents from the container

## Error Handling

The server includes comprehensive error handling for:
- Connection failures
- Authentication errors
- Invalid queries
- Document not found scenarios
- Partition key mismatches

## Security Considerations

- Store sensitive credentials in environment variables
- Use Azure Key Vault for production deployments
- Implement proper access controls on your Cosmos DB account
- Regularly rotate access keys

## Development

### Project Structure
```
├── main.py              # Main MCP server implementation
├── requirements.txt     # Python dependencies
├── .env.example        # Environment template
├── .gitignore          # Git ignore patterns
└── README.md           # This file
```

### Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## Troubleshooting

### Common Issues

1. **Connection Failed**: Verify your endpoint URL and access key
2. **Database/Container Not Found**: Ensure the names match exactly
3. **Partition Key Errors**: Verify partition key values for read/update/delete operations
4. **Query Timeouts**: Enable cross-partition queries for complex queries

### Logging

The server uses Python's logging module. Set log level via environment:
```bash
export PYTHONPATH=.
export LOG_LEVEL=DEBUG
python main.py
```

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Support

For issues and questions:
- Create an issue in the GitHub repository
- Check Azure Cosmos DB documentation
- Review MCP specification at https://modelcontextprotocol.io/
