from tortoise import Model, fields


class User(Model):
    id = fields.IntField(primary_key=True)
    username = fields.TextField(max_length=50)
    email = fields.CharField(max_length=100, unique=True)
    hashed_password = fields.TextField(max_length=128)
    created_at = fields.DatetimeField(auto_now = True)
class Task(Model): 
    id = fields.IntField(primary_key=True)
    title = fields.CharField(max_length= 255) 
    description = fields.TextField(max_length= 255)
    completed = fields.BooleanField(default=False)
    owner= fields.ForeignKeyField('models.User', related_name='tasks', on_delete=fields.CASCADE)
    created_at = fields.DatetimeField(auto_now_add = True)
    updated_at = fields.DatetimeField(auto_now = True)
    




























