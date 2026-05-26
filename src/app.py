import json
import os
from datetime import datetime, timezone

import pika
from fastapi import Depends, FastAPI, HTTPException, Response
from sqlalchemy import text
from sqlalchemy.orm import Session
from src.database import Base, engine, get_db
from src.models import Node
from src.schemas import NodeCreate, NodeResponse, NodeUpdate

Base.metadata.create_all(bind=engine)
app = FastAPI()

QUEUE_NAME = "node_events"


def publish_event(event: str, node_name: str) -> None:
    """Publish a node event to RabbitMQ."""
    rabbitmq_url = os.environ.get("RABBITMQ_URL", "amqp://guest:guest@rabbitmq:5672/")
    params = pika.URLParameters(rabbitmq_url)
    connection = pika.BlockingConnection(params)
    channel = connection.channel()
    channel.queue_declare(queue=QUEUE_NAME, durable=True)
    message = json.dumps({
        "event": event,
        "node_name": node_name,
        "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    })
    channel.basic_publish(
        exchange="",
        routing_key=QUEUE_NAME,
        body=message,
        properties=pika.BasicProperties(delivery_mode=2),  # persistent
    )
    connection.close()


@app.get("/health")
def health(db: Session = Depends(get_db)):
    try:
        db.execute(text("SELECT 1"))
        db_status = "connected"
    except Exception:
        db_status = "disconnected"
    count = db.query(Node).filter(Node.status == "active").count()
    return {"status": "ok", "db": db_status, "nodes_count": count}


@app.post("/api/nodes", response_model=NodeResponse, status_code=201)
def register_node(node: NodeCreate, db: Session = Depends(get_db)):
    existing = db.query(Node).filter(Node.name == node.name).first()
    if existing:
        raise HTTPException(status_code=409, detail="Node already exists")
    db_node = Node(name=node.name, host=node.host, port=node.port)
    db.add(db_node)
    db.commit()
    db.refresh(db_node)
    publish_event("node_registered", db_node.name)
    return db_node


@app.get("/api/nodes", response_model=list[NodeResponse])
def list_nodes(db: Session = Depends(get_db)):
    return db.query(Node).all()


@app.get("/api/nodes/{name}", response_model=NodeResponse)
def get_node(name: str, db: Session = Depends(get_db)):
    node = db.query(Node).filter(Node.name == name).first()
    if not node:
        raise HTTPException(status_code=404, detail="Node not found")
    return node


@app.put("/api/nodes/{name}", response_model=NodeResponse)
def update_node(name: str, update: NodeUpdate, db: Session = Depends(get_db)):
    node = db.query(Node).filter(Node.name == name).first()
    if not node:
        raise HTTPException(status_code=404, detail="Node not found")
    if update.host is not None:
        node.host = update.host
    if update.port is not None:
        node.port = update.port
    node.updated_at = datetime.now(timezone.utc)
    db.commit()
    db.refresh(node)
    return node


@app.delete("/api/nodes/{name}", status_code=204)
def delete_node(name: str, db: Session = Depends(get_db)):
    node = db.query(Node).filter(Node.name == name).first()
    if not node:
        raise HTTPException(status_code=404, detail="Node not found")
    node.status = "inactive"
    node.updated_at = datetime.now(timezone.utc)
    db.commit()
    publish_event("node_deleted", node.name)
    return Response(status_code=204)

