from pydantic import BaseModel, EmailStr

class Product(BaseModel):
    name: str
    price: float
    email: EmailStr

class ProductWithStatus(Product):
    id: int
    status: str


