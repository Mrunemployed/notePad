import os
import bson.errors
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorCollection, AsyncIOMotorDatabase, AsyncIOMotorCursor
from pydantic import BaseModel, TypeAdapter, ValidationError
from app.db import db_models
import json
from typing import Any, Coroutine, AsyncGenerator, Union, TypeVar, Type, Optional, Dict
from dataclasses import dataclass
import asyncio
from asyncio import Task
from enum import Enum
from bson import ObjectId
import bson
from app.db.mdbidGen import SnowflakeU64
from asyncio import AbstractEventLoop
import inspect

MONGO_URL = os.getenv("MONGO_URL", "mongodb://localhost:27017")
# MONGO_URL = "mongodb://localhost:27017"

DB_NAME = os.getenv("MONGO_DB_NAME", "medusa")
print(MONGO_URL)

T = TypeVar("T", bound=BaseModel)

class Operations(Enum):
    insert:str
    update:str
    delete:str

@dataclass
class OperationsCache:
    operation:Operations
    dbName:str
    CollectionName:str
    Data: Any
    
class TaskRegistry:
    def __init__(self):
        self.operations: list[OperationsCache]
        self._task_mapper: dict[int,Coroutine]
    
    def _scheduler(self):
        ...
    

class mapis:

    def __init__(
        self,
        dataBase:str=None,
        idType:str=None,
        nodeId:int=None
    ):
        mongourl = MONGO_URL
        self._snowFlu64 : SnowflakeU64 = None
        self.async_client = AsyncIOMotorClient(mongourl)
        self.async_engine = self._init_db_(dataBase)
        if idType and nodeId:
            self._idType:str = self._set_id_type(idType=idType,nodeId=nodeId)
        else:
            self._idType:str = self._set_id_type()


    def _set_id_type(self,nodeId:int=None,idType:str="default")-> str:
        """A helper called at initialization to set the type of IDs getting generated"""
        if not isinstance(idType,str):
            raise ValueError(f"{idType!r} can only be a string")
        if idType == "default":
            self._idType = "default"
        elif idType == "uint64" and nodeId:
            self._snowFlu64 = SnowflakeU64(node_id=nodeId)
            idType = "uint64"
        else:
            raise ValueError(f"idType can only be `uint64` or `default` not {idType!r}")
        return idType

    def _init_db_(self, dataBase:str=None)-> AsyncIOMotorDatabase:
        _database = dataBase or DB_NAME
        if not _database:
            raise RuntimeError("The database name cannot be empty")
        async_engine = self.async_client.get_database(DB_NAME)
        return async_engine

    async def ping(self) -> bool:
        _ping = await self.async_client.admin.command('ping')
        if _ping:
            print("ping:",_ping)
            return True
        else:
            return False

    def _verify_model(self,modelClass:BaseModel):
        if inspect.isclass(modelClass):
            modelName = modelClass.__name__
        else:
            modelName = modelClass.__class__.__name__
        all_models = [x for x in dir(db_models) if not x.startswith('__')]
        if modelName in all_models:
            return True
        return False
    
    
    async def _existing_collection(self,name:str):
        all_existing = await self.async_engine.list_collection_names()
        return name in all_existing

    async def create_collection(self,collection:BaseModel):
        """
        Creates a new pydantic enforced collection
        Args:
            collection(BaseModel): The pydantic collection class reference
        Returns:
            status(None): Does not return anything raises exception if creation failed.
        Raises:
            (Exception): If the collection is not a valid Pydantic class or if the class has not been defined
        
        """
        collection = self._fetch_model(collection)
        _model_name = collection.__name__

        if not self._fetch_model(collection):
            raise ValueError(f"Collection: {_model_name!r} has to be a Pydantic Class!")
        if not self._verify_model(collection):
            raise ValueError(f"Collection: {_model_name!r} needs to have a valid Pydantic Class created in `db_models`")
        if await self._existing_collection(_model_name):
            raise TypeError(f"Collection: {_model_name!r} already exists!")
        # print(collection.model_json_schema())
        _validator = {
            "$jsonSchema":collection.model_json_schema()
            }
        await self.async_engine.create_collection(
                _model_name,
                # validator=_validator,
                # validationLevel="strict",     # or "moderate"
                # validationAction="error"      # or "warn"
            )
        
    async def __modify_collection(self, collection:BaseModel):
        """
        Modifies an existing pydantic enforced collection
        Args:
            collection(BaseModel): The pydantic collection class reference
        Returns:
            status(None): Does not return anything raises exception if creation failed.
        Raises:
            (Exception): If the collection is not a valid Pydantic class or if the class has not been defined
        
        """
        collection = self._fetch_model(collection)
        if not self._fetch_model(collection):
            raise ValueError(f"Collection: {_model_name!r} has to be a Pydantic Class!")
        if not self._verify_model(collection):
            raise ValueError(f"Collection: {_model_name!r} needs to have a valid Pydantic Class created in `db_models`")
        _model_name = collection.__name__
        _validator = {
            "$jsonSchema":json.loads(collection.model_json_schema())
            }
        if await self._existing_collection(_model_name):
            await self.async_engine.command({
                "collMod": "users",
                "validator": _validator,
                "validationLevel": "moderate",   # allow existing docs, enforce on updates/inserts
                "validationAction": "warn"       # only log violations
            })

    def _getdb_cursor(self,name:Union[str,BaseModel]) -> AsyncIOMotorCollection:
        """
        Fetches the reference Collection (Database Table reference) from the Database
        Args:
            name(str): The name of the model or Database
        Returns:
            out(AsyncIOMotorCollection): The AsyncIOMotorCollection object reference
        """
        if self._is_pydt_class(name):
            name = name.__name__
        cursor = getattr(self.async_engine,name)
        return cursor
    
    def _is_pydt_class(self,cls)->bool:
        if not inspect.isclass(cls):
            cls = cls.__class__
        return inspect.isclass(cls) and issubclass(cls,BaseModel)


    def _fetch_model(self,name:str) -> BaseModel:
        """
        Fetches the model from the declared db models in `db_models` module
        Args:
            name(str): The name of the model
        Returns:
            out(BaseModel): The pydantic model that
        
        """
        if not isinstance(name,str):
            if self._is_pydt_class(name):
                name = name.__name__

        model = getattr(db_models,name)
        if not model:
            raise ValueError(f"{name!r}is an Invalid Model name or the Model does not exist")
        return model


    def _validate(self,modelObj:BaseModel) -> tuple[BaseModel,str]:
        """
        Validates if the values are consistent with the Pydantic model described in `db_models` module.
        Args:
            model(str): The name of the pydantic model
            values(dict): The values for the pydantic model passed on as kwargs
        Returns:
            out(BaseModel): The pydantic model object of the declared pydantic class  
        
        """
        if not self._is_pydt_class(modelObj):
            raise ValueError(f"{modelObj!r} is not a valid Pydantic class")
        
        model_name = modelObj.__class__.__name__
        pydtclass = self._fetch_model(model_name)
        # print("fields:",pydtclass.__annotations__)
        return modelObj,model_name
    
    async def _safe_load_id(self, _id:Union[str,int]) -> Union[ObjectId,int]:
        if isinstance(_id, str) and self._idType == "default":
            try:
                _id = ObjectId(_id)
            except bson.errors.InvalidId:
                _id = int(_id)
        elif isinstance(_id, int) or self._idType == "uint64":
            try:
                _id = int(_id)
            except ValueError:
                _id = ObjectId(_id)
        return _id

    async def _verify_relation(
        self,
        relation:BaseModel,
        _id:str
    )-> Union[None,str]:
        # records = await self.findn(model=relation, filter={'_id':_id}, n=2)
        exists = await self.async_engine[relation.__name__].count_documents({"_id": _id}, limit=2)
        print("FOUND RECORDS AT `_verify_relation`:",exists)
        if exists == 1:
            return _id
        else:
            raise ValueError(f"Too many values found, `_id`={_id!r} not a unique ID - Relations can only be established when the ID is unique")
        
    async def insert(self,
        modelObj:Type[T],
        relation:BaseModel=None,
        _id:str=None,
        unique:Union[dict,None]=None
    )-> Union[str,int]:
        if relation is None and _id:
            raise ValueError(f"relation cannot be empty when _id={_id!r}!")
        if _id is None and relation:
            raise ValueError(f"if relation={relation.__name__!r} _id cannot be empty!")
        if relation and _id:
            await self._verify_relation(relation,_id)
        if unique:
            _existing = await self.find(model=modelObj.__class__, filter=unique)
            if _existing:
                print("Record already exists!")
                return None
        _,model_name = self._validate(modelObj)
        cursor = self._getdb_cursor(model_name)
        record = modelObj.model_dump()
        if self._idType == "uint64" and record.get("_id") is None:
            uid = await self._snowFlu64()
            record.update({"_id": uid})
            # object.__setattr__(modelObj, "_id",uid)
        _doc_id = await cursor.insert_one(record)
        if self._idType == "uint64":
            return int(_doc_id.inserted_id)
        return str(_doc_id.inserted_id)

    async def _perform_task(self, taskList:list[Coroutine|Task]):
        await asyncio.gather(taskList)

    async def insertn(self, records:list[Type[T]]):
        if not isinstance(list):
            raise TypeError(f"{records!r} has to be a List!")
        _tasks = []
        for i in records:
            if not self._is_pydt_class(i):
                raise TypeError(f"{i!r} is not a valid Pydantic Object!")
            if not self._verify_model(i):
                raise NotImplementedError(f"{i!r} belongs to an urecognized Pydantic Model!")
            
            Texecute = self.insert(i)
            _tasks.append(Texecute)

        await self._perform_task(_tasks)

    
    async def find(self, model:Type[T], filter:dict)->Optional[Union[T,None]]:
        if '_id' in filter:
            filter['_id'] = await self._safe_load_id(filter['_id'])
        
        cursor = self._getdb_cursor(name=model)
        value = await cursor.find_one(filter=filter)

        if value is None:
            return None
        instance = model.model_construct(
            **value,
            _fields_set=set(value.keys()),   # mark only these as "present"
        )
        if self._idType == "uint64":
            try:
                object.__setattr__(instance, "_id", int(value.get("_id")))
            except TypeError:
                object.__setattr__(instance, "_id", str(value.get("_id")))
        else:
            object.__setattr__(instance, "_id", str(value.get("_id")))

        return instance
     
    async def sfindn(self,model:Type[T], filter:dict={}, n:int=None, skip:int=None)->AsyncGenerator[Optional[T], Any]:
        if '_id' in filter:
            filter['_id'] = await self._safe_load_id(filter['_id'])

        cursor = self._getdb_cursor(name=model)
        mcursor = cursor.find(filter)
        if skip:
            mcursor = mcursor.skip(skip=skip)
        async for doc in mcursor:
            docObj = model.model_construct(
            **doc,
            _fields_set=set(doc.keys()),
            )
            if self._idType == "uint64":
                try:
                    object.__setattr__(docObj, "_id", int(doc.get("_id")))
                except TypeError:
                    object.__setattr__(docObj, "_id", str(doc.get("_id")))
            else:
                object.__setattr__(docObj, "_id", str(doc.get("_id")))

            if n is not None and n >=0:
                n-=1
                yield docObj
            elif n == 0:
                break
            elif n is None:
                yield docObj

    async def findn(self, model:Type[T], filter:dict={}, n:int=None, skip:int=None)->Optional[list[T]]:
        if '_id' in filter:
            filter['_id'] = await self._safe_load_id(filter['_id'])
            
        cursor = self._getdb_cursor(name=model)
        mcursor = cursor.find(filter)
        if skip:
            all_docs = await mcursor.skip(skip=skip)
        
        if n:
            all_docs = await mcursor.skip().to_list(length=n)
        else:
            all_docs = await mcursor.to_list()
        struct_docs = []
        for doc in all_docs:
            _model = model.model_construct(**doc,_fields_set=set(doc.keys()))
            if self._idType == "uint64":
                try:
                    object.__setattr__(model, "_id", int(doc.get("_id")))
                except TypeError:
                    object.__setattr__(_model, "_id", str(doc.get("_id")))
            else:
                object.__setattr__(_model, "_id", str(doc.get("_id")))
            struct_docs.append(_model)
        return struct_docs
    
    async def update(
        self,
        model: Type[T],
        filter: dict,
        updates: Dict[str, Any],
        *,
        many: bool = False,
        upsert: bool = False,
    ) -> int:
        """
        Loosely update only the keys in `updates`:
        Args:
            model:  a Pydantic BaseModel class from db_models
            filter: a Mongo filter
            updates: a plain dict of new values
            many:   if True, do update_many; else update_one
            upsert: if True, insert when no match found

        Returns the number of documents modified.
        """

        # 1) Validate model argument
        if not inspect.isclass(model) or not issubclass(model, BaseModel):
            raise ValueError(f"{model!r} is not a Pydantic model class")
        if not self._verify_model(model):
            raise ValueError(f"{model.__name__!r} is not one of your db_models")
        if '_id' in filter:
            filter['_id'] = await self._safe_load_id(filter['_id'])

        # 2) Prepare a sanitized dict to $set
        if self._is_pydt_class(updates):
            updates = updates.model_dump(mode='python')
        to_set: Dict[str, Any] = {}
        print(updates, type(updates))
        for key, raw_value in updates.items():
            # 2a) must be a declared field
            fields = model.model_fields
            if key not in fields:
                raise KeyError(f"Unknown field {key!r} for model {model.__name__}")

            field_info = fields[key]
            ann = field_info.annotation

            # 2b) nested BaseModel?
            if inspect.isclass(ann) and issubclass(ann, BaseModel):
                # accept dict or instance
                if isinstance(raw_value, dict):
                    try:
                        inst = ann.model_validate(raw_value)
                    except ValidationError as e:
                        raise ValueError(f"Bad data for nested {key}: {e}") from e
                elif isinstance(raw_value, ann):
                    inst = raw_value
                else:
                    raise TypeError(
                        f"Field {key!r} expects {ann.__name__} or dict, got {type(raw_value).__name__}"
                    )
                to_set[key] = inst.model_dump()

            else:
                # 2c) primitive or other: use TypeAdapter to coerce & validate
                try:
                    adapter = TypeAdapter(ann)
                    validated = adapter.validate_python(raw_value)
                except ValidationError as e:
                    raise ValueError(f"Type mismatch for field {key!r}: {e}") from e
                to_set[key] = validated

        # 3) Fire the Mongo update
        coll = self._getdb_cursor(model.__name__)
        if many:
            res = await coll.update_many(filter, {"$set": to_set}, upsert=upsert)
        else:
            res = await coll.update_one(filter, {"$set": to_set}, upsert=upsert)

        return res.modified_count


    async def delete(
        self,
        model: Union[str, BaseModel],
        filter: dict,
        many: bool = False
    ) -> int:
        """
        Delete document(s) matching the filter.
        - model: either the BaseModel class or its name as string
        - filter: Mongo‐style filter to locate which docs to delete
        - many: if True, uses delete_many; otherwise delete_one

        Returns the number of documents deleted.
        """
        # figure out the model name
        if isinstance(model, BaseModel):
            model_name = model.__class__.__name__
        elif inspect.isclass(model) and issubclass(model, BaseModel):
            model_name = model.__name__
        else:
            model_name = model

        # verify it’s one of your declared Pydantic models
        if not self._verify_model(self._fetch_model(model_name)):
            raise ValueError(f"{model_name!r} is not a recognized Pydantic model")
        
        if '_id' in filter:
            filter['_id'] = await self._safe_load_id(filter['_id'])

        coll = self._getdb_cursor(model_name)
        if many:
            result = await coll.delete_many(filter)
        else:
            result = await coll.delete_one(filter)

        return result.deleted_count
    
    
    async def ensure_collections(self, collection:BaseModel):
        collection = self._fetch_model(collection)
        _model_name = collection.__name__
        if not self._verify_model(collection):
            raise ValueError(f"Collection: {_model_name!r} needs to have a valid Pydantic Class created in `db_models`")
        if await self._existing_collection(_model_name):
            return
        else:
            await self.create_collection(collection=collection)
            
    async def create_all_collections(self):
        try:
            models = dir(db_models)
            classes = [x for x in models if not x.startswith('__')]
            for cls in classes:
                model = self._fetch_model(cls)
                await self.create_collection(model)
        except Exception as e:
            print(f"[WARN] Skipping creating collection - {cls!r} :{e}")

if __name__ == '__main__':

    dbengine = mapis('trial')

    # asyncio.run(dbengine.create_collection(db_models.test))
    from db_models import test, Hobbies
    h = Hobbies(name='coding', frequency=365)
    t = test(name='rudra', age=21, hobbies=h)
    # asyncio.run(dbengine.insert1(t))

    async def streamone():
        val = await dbengine.find(model=test, filter={"name":"rudra"})
        print(val)
        values = dbengine.sfindn(model=test)
        async for val in values:
            print(val.hobbies, type(val))

    asyncio.run(streamone())