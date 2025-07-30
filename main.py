import os
import asyncio
import logging
from typing import Dict, List, Any, Optional
from dataclasses import dataclass

from azure.cosmos.aio import CosmosClient
from azure.cosmos import PartitionKey, exceptions
from dotenv import load_dotenv
from pydantic import AnyUrl

from mcp.server.models import InitializationOptions
from mcp.server import NotificationOptions, Server
from mcp.types import (
    Resource,
    Tool,
    TextContent,
    ImageContent,
    EmbeddedResource,
    LoggingLevel
)
import mcp.types as types
from mcp.server.session import ServerSession
from mcp.server.stdio import stdio_server

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class CosmosConfig:
    """Configuration for Azure Cosmos DB connection"""
    endpoint: str
    key: str
    database_name: str
    container_name: str
    consistency_level: str = "Session"
    connection_mode: str = "Gateway"

class CosmosDBMCPServer:
    """MCP Server for Azure Cosmos DB operations"""
    
    def __init__(self):
        self.config = self._load_config()
        self.cosmos_client: Optional[CosmosClient] = None
        self.database = None
        self.container = None
        self.server = Server("cosmosdb-mcp-server")
        self._setup_handlers()
    
    def _load_config(self) -> CosmosConfig:
        """Load configuration from environment variables"""
        return CosmosConfig(
            endpoint=os.getenv("COSMOS_ENDPOINT", ""),
            key=os.getenv("COSMOS_KEY", ""),
            database_name=os.getenv("COSMOS_DATABASE_NAME", ""),
            container_name=os.getenv("COSMOS_CONTAINER_NAME", ""),
        )
    
    async def _initialize_cosmos_client(self):
        """Initialize Azure Cosmos DB client and verify connection"""
        try:
            self.cosmos_client = CosmosClient(
                url=self.config.endpoint,
                credential=self.config.key
            )
            
            # Get database reference
            self.database = self.cosmos_client.get_database_client(self.config.database_name)
            
            # Get container reference
            self.container = self.database.get_container_client(self.config.container_name)
            
            # Test connection
            await self.database.read()
            logger.info(f"Successfully connected to Cosmos DB: {self.config.database_name}")
            
        except Exception as e:
            logger.error(f"Failed to initialize Cosmos DB client: {str(e)}")
            raise
    
    def _setup_handlers(self):
        """Setup MCP server handlers"""
        
        @self.server.list_resources()
        async def handle_list_resources() -> list[Resource]:
            """List available Cosmos DB resources"""
            return [
                Resource(
                    uri=AnyUrl("cosmosdb://database"),
                    name="Database Info",
                    description="Information about the connected Cosmos DB database",
                    mimeType="application/json"
                ),
                Resource(
                    uri=AnyUrl("cosmosdb://container"),
                    name="Container Info", 
                    description="Information about the connected Cosmos DB container",
                    mimeType="application/json"
                ),
                Resource(
                    uri=AnyUrl("cosmosdb://documents"),
                    name="Documents",
                    description="Access to documents in the container",
                    mimeType="application/json"
                )
            ]
        
        @self.server.read_resource()
        async def handle_read_resource(uri: AnyUrl) -> str:
            """Read a specific Cosmos DB resource"""
            if not self.cosmos_client:
                await self._initialize_cosmos_client()
            
            if not self.database or not self.container:
                raise RuntimeError("Database or container not initialized")
            
            uri_str = str(uri)
            if uri_str == "cosmosdb://database":
                db_info = await self.database.read()
                return f"Database: {db_info['id']}\nCreated: {db_info.get('_ts', 'N/A')}"
            
            elif uri_str == "cosmosdb://container":
                container_info = await self.container.read()
                return f"Container: {container_info['id']}\nPartition Key: {container_info.get('partitionKey', 'N/A')}"
            
            elif uri_str == "cosmosdb://documents":
                # Return a sample of documents
                items = []
                async for item in self.container.query_items(
                    query="SELECT TOP 10 * FROM c"
                ):
                    items.append(item)
                return f"Sample documents (first 10):\n{items}"
            
            else:
                raise ValueError(f"Unknown resource URI: {uri_str}")
        
        @self.server.list_tools()
        async def handle_list_tools() -> list[Tool]:
            """List available Cosmos DB tools"""
            return [
                Tool(
                    name="query_documents",
                    description="Execute a SQL query against Cosmos DB documents",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "query": {
                                "type": "string",
                                "description": "SQL query to execute"
                            },
                            "parameters": {
                                "type": "array",
                                "items": {"type": "object"},
                                "description": "Optional query parameters"
                            },
                            "cross_partition": {
                                "type": "boolean",
                                "description": "Enable cross-partition query",
                                "default": True
                            }
                        },
                        "required": ["query"]
                    }
                ),
                Tool(
                    name="create_document",
                    description="Create a new document in Cosmos DB",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "document": {
                                "type": "object",
                                "description": "Document to create"
                            },
                            "partition_key": {
                                "type": "string",
                                "description": "Partition key value (if different from id)"
                            }
                        },
                        "required": ["document"]
                    }
                ),
                Tool(
                    name="read_document",
                    description="Read a specific document by ID",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "document_id": {
                                "type": "string",
                                "description": "Document ID to read"
                            },
                            "partition_key": {
                                "type": "string",
                                "description": "Partition key value"
                            }
                        },
                        "required": ["document_id", "partition_key"]
                    }
                ),
                Tool(
                    name="update_document",
                    description="Update an existing document",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "document_id": {
                                "type": "string",
                                "description": "Document ID to update"
                            },
                            "document": {
                                "type": "object",
                                "description": "Updated document data"
                            },
                            "partition_key": {
                                "type": "string",
                                "description": "Partition key value"
                            }
                        },
                        "required": ["document_id", "document", "partition_key"]
                    }
                ),
                Tool(
                    name="delete_document",
                    description="Delete a document by ID",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "document_id": {
                                "type": "string",
                                "description": "Document ID to delete"
                            },
                            "partition_key": {
                                "type": "string",
                                "description": "Partition key value"
                            }
                        },
                        "required": ["document_id", "partition_key"]
                    }
                ),
                Tool(
                    name="get_container_statistics",
                    description="Get statistics about the container",
                    inputSchema={
                        "type": "object",
                        "properties": {},
                        "required": []
                    }
                )
            ]
        
        @self.server.call_tool()
        async def handle_call_tool(name: str, arguments: dict) -> list[types.TextContent | types.ImageContent | types.EmbeddedResource]:
            """Handle tool calls"""
            if not self.cosmos_client:
                await self._initialize_cosmos_client()
            
            try:
                if name == "query_documents":
                    result = await self._query_documents(arguments)
                elif name == "create_document":
                    result = await self._create_document(arguments)
                elif name == "read_document":
                    result = await self._read_document(arguments)
                elif name == "update_document":
                    result = await self._update_document(arguments)
                elif name == "delete_document":
                    result = await self._delete_document(arguments)
                elif name == "get_container_statistics":
                    result = await self._get_container_statistics()
                else:
                    raise ValueError(f"Unknown tool: {name}")
                
                return [TextContent(type="text", text=str(result))]
                
            except Exception as e:
                error_msg = f"Error executing {name}: {str(e)}"
                logger.error(error_msg)
                return [TextContent(type="text", text=error_msg)]
    
    async def _query_documents(self, arguments: dict) -> dict:
        """Execute a query against Cosmos DB"""
        if not self.container:
            raise RuntimeError("Container not initialized")
            
        query = arguments["query"]
        parameters = arguments.get("parameters", [])
        cross_partition = arguments.get("cross_partition", True)
        
        items = []
        async for item in self.container.query_items(
            query=query,
            parameters=parameters
        ):
            items.append(item)
        
        return {
            "query": query,
            "result_count": len(items),
            "documents": items
        }
    
    async def _create_document(self, arguments: dict) -> dict:
        """Create a new document"""
        if not self.container:
            raise RuntimeError("Container not initialized")
            
        document = arguments["document"]
        partition_key = arguments.get("partition_key")
        
        if "id" not in document:
            import uuid
            document["id"] = str(uuid.uuid4())
        
        created_item = await self.container.create_item(
            body=document,
            partition_key=partition_key
        )
        
        return {
            "status": "created",
            "document_id": created_item["id"],
            "document": created_item
        }
    
    async def _read_document(self, arguments: dict) -> dict:
        """Read a document by ID"""
        if not self.container:
            raise RuntimeError("Container not initialized")
            
        document_id = arguments["document_id"]
        partition_key = arguments["partition_key"]
        
        document = await self.container.read_item(
            item=document_id,
            partition_key=partition_key
        )
        
        return {
            "status": "found",
            "document": document
        }
    
    async def _update_document(self, arguments: dict) -> dict:
        """Update an existing document"""
        if not self.container:
            raise RuntimeError("Container not initialized")
            
        document_id = arguments["document_id"]
        document = arguments["document"]
        partition_key = arguments["partition_key"]
        
        # Ensure the document has the correct ID
        document["id"] = document_id
        
        updated_item = await self.container.replace_item(
            item=document_id,
            body=document,
            partition_key=partition_key
        )
        
        return {
            "status": "updated",
            "document": updated_item
        }
    
    async def _delete_document(self, arguments: dict) -> dict:
        """Delete a document by ID"""
        if not self.container:
            raise RuntimeError("Container not initialized")
            
        document_id = arguments["document_id"]
        partition_key = arguments["partition_key"]
        
        await self.container.delete_item(
            item=document_id,
            partition_key=partition_key
        )
        
        return {
            "status": "deleted",
            "document_id": document_id
        }
    
    async def _get_container_statistics(self) -> dict:
        """Get container statistics"""
        if not self.container:
            raise RuntimeError("Container not initialized")
            
        try:
            # Get container properties
            container_props = await self.container.read()
            
            # Count documents (this might be expensive for large containers)
            count_query = "SELECT VALUE COUNT(1) FROM c"
            count_result = []
            async for item in self.container.query_items(
                query=count_query
            ):
                count_result.append(item)
            
            document_count = count_result[0] if count_result else 0
            
            return {
                "container_id": container_props["id"],
                "partition_key": container_props.get("partitionKey", {}),
                "document_count": document_count,
                "indexing_policy": container_props.get("indexingPolicy", {}),
                "created_timestamp": container_props.get("_ts")
            }
            
        except Exception as e:
            return {
                "error": f"Failed to get statistics: {str(e)}"
            }
    
    async def run(self):
        """Run the MCP server"""
        # Validate configuration
        if not all([self.config.endpoint, self.config.key, self.config.database_name, self.config.container_name]):
            raise ValueError("Missing required Cosmos DB configuration. Please check your environment variables.")
        
        # Initialize Cosmos DB connection
        await self._initialize_cosmos_client()
        
        # Run the server
        async with stdio_server() as (read_stream, write_stream):
            await self.server.run(
                read_stream,
                write_stream,
                InitializationOptions(
                    server_name="cosmosdb-mcp-server",
                    server_version="1.0.0",
                    capabilities=self.server.get_capabilities(
                        notification_options=NotificationOptions(),
                        experimental_capabilities={}
                    )
                )
            )

async def main():
    """Main entry point"""
    server = CosmosDBMCPServer()
    await server.run()

if __name__ == "__main__":
    asyncio.run(main())
