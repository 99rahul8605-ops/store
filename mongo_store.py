"""MongoDB implementation of the FileBot's small, fixed persistence interface.

Only the explicitly known statements from bot.py are recognized. Do not pass user SQL.
All token claims use a single atomic find_one_and_update operation.
"""
from __future__ import annotations

import asyncio
import re
from contextlib import asynccontextmanager
from contextvars import ContextVar
from datetime import datetime, timedelta, timezone
from pymongo import AsyncMongoClient, ASCENDING, DESCENDING, ReturnDocument
from pymongo.errors import DuplicateKeyError


def now():
    return datetime.now(timezone.utc)


def clean(doc):
    if doc is None: return None
    return {k: v for k, v in doc.items() if k != '_id'}


class MongoStore:
    def __init__(self, uri: str, name: str):
        if not (uri.startswith('mongodb+srv://') or uri.startswith('mongodb://')):
            raise ValueError('MONGODB_URI must be a MongoDB connection string')
        if not re.fullmatch(r'[A-Za-z0-9_-]{1,64}', name):
            raise ValueError('Invalid MONGODB_DB name')
        self.client = AsyncMongoClient(uri, serverSelectionTimeoutMS=10000, tz_aware=True, tls=True)
        self.db = self.client[name]
        self._session = ContextVar('mongo_session', default=None)

    def col(self, name):
        return self.db[name]

    def kw(self):
        s = self._session.get()
        return {'session': s} if s else {}

    async def setup(self):
        await self.client.admin.command('ping')
        indexes = {
            'blbot_settings': [('key', 1)], 'blbot_sudo':[('user_id',1)],
            'blbot_packages':[('id',1)], 'blbot_items':[('package_id',1),('position',1)],
            'blbot_ingested':[('source_chat_id',1),('source_message_id',1)],
            'blbot_drafts':[('uploader_id',1)], 'blbot_force_chats':[('chat_id',1)],
            'blbot_join_requests':[('chat_id',1),('user_id',1)],
            'blbot_unlocks':[('token_hash',1)], 'blbot_pending':[('user_id',1)],
            'blbot_admin_input':[('user_id',1)], 'blbot_sent_media':[('chat_id',1),('message_id',1)],
        }
        for collection, fields in indexes.items():
            await self.col(collection).create_index(fields, unique=True)
        await self.col('blbot_unlocks').create_index([('user_id',1),('package_id',1),('created_at',-1)])
        await self.col('blbot_sent_media').create_index([('delete_at',1),('next_try_at',1)])
        await self.col('blbot_packages').create_index([('created_at',-1)])
        await self.col('blbot_items').create_index([('package_id',1),('position',1)])

    async def close(self):
        await self.client.close()

    @asynccontextmanager
    async def acquire(self):
        yield self

    @asynccontextmanager
    async def transaction(self):
        # Atlas replica sets support multi-document transactions.
        # The ContextVar keeps each concurrent Telegram handler's session isolated.
        async with self.client.start_session() as session:
            async with await session.start_transaction():
                token = self._session.set(session)
                try:
                    yield self
                finally:
                    self._session.reset(token)

    async def fetchrow(self, sql, *p):
        q = ' '.join(sql.lower().split())
        kw = self.kw()
        c = self.col
        if q.startswith('select * from blbot_packages where id='):
            return clean(await c('blbot_packages').find_one({'id':p[0],'published':True}, **kw))
        if q.startswith('select p.id,p.published from blbot_ingested'):
            found = await c('blbot_ingested').find_one({'source_chat_id':p[0],'source_message_id':p[1]},**kw)
            return clean(await c('blbot_packages').find_one({'id':found['package_id']},**kw)) if found else None
        if q.startswith('select package_id from blbot_drafts'):
            return clean(await c('blbot_drafts').find_one({'uploader_id':p[0]},**kw))
        if q.startswith('select short_url from blbot_unlocks'):
            return clean(await c('blbot_unlocks').find_one({'user_id':p[0],'package_id':p[1],
                'status':'issued','expires_at':{'$gt':now()},'short_url':{'$type':'string'}},
                sort=[('created_at',DESCENDING)],**kw))
        if q.startswith('update blbot_unlocks set status=') and 'returning *' in q:
            # Atomic claim: exactly one concurrent redemption can change issued -> delivering.
            return clean(await c('blbot_unlocks').find_one_and_update(
                {'token_hash':p[0],'user_id':p[1],'status':'issued','expires_at':{'$gt':now()}},
                [{'$set':{'status':'delivering','claimed_at':{'$ifNull':['$claimed_at',now()]}}}],
                return_document=ReturnDocument.AFTER,**kw))
        if q.startswith('select * from blbot_unlocks where token_hash='):
            return clean(await c('blbot_unlocks').find_one({'token_hash':p[0],'user_id':p[1]},**kw))
        if q.startswith('select (select count(*)'):
            return {'packages':await c('blbot_packages').count_documents({'published':True},**kw),
                    'videos':await c('blbot_items').count_documents({},**kw),
                    'completed_unlocks':await c('blbot_unlocks').count_documents({'status':'used'},**kw)}
        if q.startswith('select action from blbot_admin_input'):
            return clean(await c('blbot_admin_input').find_one({'user_id':p[0],
                'created_at':{'$gt':now()-timedelta(minutes=10)}},**kw))
        if q.startswith('select * from blbot_pending'):
            return clean(await c('blbot_pending').find_one({'user_id':p[0]},**kw))
        if q.startswith('select token_hash,package_id from blbot_unlocks'):
            # Prefix is generated from a SHA-256 hash and is never arbitrary regex.
            prefix = p[0].rstrip('%')
            return clean(await c('blbot_unlocks').find_one({'token_hash':{'$regex':'^'+re.escape(prefix)},
                'user_id':p[1],'status':'delivering'},sort=[('claimed_at',DESCENDING)],**kw))
        raise NotImplementedError('Unsupported fetchrow operation: '+q[:120])

    async def fetchval(self, sql, *p):
        q=' '.join(sql.lower().split()); c=self.col; kw=self.kw()
        if q.startswith('select value from blbot_settings'):
            row=await c('blbot_settings').find_one({'key':p[0]},**kw)
            return row['value'] if row else None
        if 'from blbot_sudo where user_id=' in q:
            return bool(await c('blbot_sudo').find_one({'user_id':p[0]},**kw))
        if 'from blbot_join_requests' in q and 'requested_at' in q:
            return bool(await c('blbot_join_requests').find_one({'chat_id':p[0],'user_id':p[1],
                'requested_at':{'$gt':now()-timedelta(hours=24)}},**kw))
        if q.startswith('select package_id from blbot_drafts'):
            row=await c('blbot_drafts').find_one({'uploader_id':p[0]},**kw)
            return row['package_id'] if row else None
        if q.startswith('select count(*) from blbot_items'):
            return await c('blbot_items').count_documents({'package_id':p[0]},**kw)
        if q.startswith('select count(*) from blbot_force_chats'):
            return await c('blbot_force_chats').count_documents({},**kw)
        if q.startswith('select count(*) from blbot_sudo'):
            return await c('blbot_sudo').count_documents({},**kw)
        if 'from blbot_force_chats' in q and 'kind=' in q:
            return bool(await c('blbot_force_chats').find_one({'chat_id':p[0],'kind':'private'},**kw))
        raise NotImplementedError('Unsupported fetchval operation: '+q[:120])

    async def fetch(self,sql,*p):
        q=' '.join(sql.lower().split()); c=self.col; kw=self.kw()
        if q.startswith('select * from blbot_force_chats'):
            return [clean(x) async for x in c('blbot_force_chats').find({},**kw).sort('name',ASCENDING)]
        if q.startswith('select user_id from blbot_sudo'):
            return [clean(x) async for x in c('blbot_sudo').find({},**kw).sort('added_at',ASCENDING)]
        if q.startswith('select position,backup_message_id,backup_chat_id from blbot_items'):
            return [clean(x) async for x in c('blbot_items').find({'package_id':p[0]},**kw).sort('position',ASCENDING)]
        if q.startswith('select p.id,count(i.position)'):
            match={'published':True}
            if 'p.uploader_id=$1' in q: match['uploader_id']=p[0]
            packages=c('blbot_packages').find(match,**kw).sort('created_at',DESCENDING)
            out=[]
            async for pkg in packages:
                count=await c('blbot_items').count_documents({'package_id':pkg['id']},**kw)
                if count:out.append({'id':pkg['id'],'total':count})
                if len(out)==10:break
            return out
        if q.startswith('select chat_id,message_id,attempts from blbot_sent_media'):
            due={'delete_at':{'$lte':now()},'$or':[{'next_try_at':None},{'next_try_at':{'$lte':now()}}]}
            return [clean(x) async for x in c('blbot_sent_media').find(due,**kw).sort('delete_at',ASCENDING).limit(50)]
        raise NotImplementedError('Unsupported fetch operation: '+q[:120])

    async def execute(self,sql,*p):
        q=' '.join(sql.lower().split()); c=self.col; kw=self.kw(); t=now()
        if q.startswith('insert into blbot_settings'):
            await c('blbot_settings').update_one({'key':p[0]},{'$set':{'value':p[1]}},upsert=True,**kw)
        elif q.startswith('insert into blbot_pending'):
            await c('blbot_pending').update_one({'user_id':p[0]},
                {'$set':{'package_id':p[1],'claim_hash':p[2],'updated_at':t}},upsert=True,**kw)
        elif q.startswith('delete from blbot_pending'):
            r=await c('blbot_pending').delete_one({'user_id':p[0]},**kw)
            return f'DELETE {r.deleted_count}'
        elif q.startswith('insert into blbot_unlocks'):
            await c('blbot_unlocks').insert_one({'token_hash':p[0],'package_id':p[1],'user_id':p[2],
                'status':'issued','delivery_cursor':0,'short_url':None,'created_at':t,'expires_at':t+timedelta(hours=3),'claimed_at':None},**kw)
        elif q.startswith('update blbot_unlocks set short_url='):
            await c('blbot_unlocks').update_one({'token_hash':p[0]},{'$set':{'short_url':p[1]}},**kw)
        elif q.startswith('delete from blbot_unlocks where token_hash='):
            await c('blbot_unlocks').delete_one({'token_hash':p[0]},**kw)
        elif q.startswith('insert into blbot_sent_media'):
            try:
                await c('blbot_sent_media').insert_one({'chat_id':p[0],'message_id':p[1],
                    'delete_at':t+timedelta(minutes=p[2]),'attempts':0,'next_try_at':None},**kw)
            except DuplicateKeyError: pass
        elif q.startswith('update blbot_unlocks set delivery_cursor='):
            await c('blbot_unlocks').update_one({'token_hash':p[0],'status':'delivering'},
                {'$max':{'delivery_cursor':p[1]}},**kw)
        elif q.startswith("update blbot_unlocks set status='used'"):
            await c('blbot_unlocks').update_one({'token_hash':p[0],'status':'delivering'},
                {'$set':{'status':'used'}},**kw)
        elif q.startswith('insert into blbot_packages'):
            await c('blbot_packages').insert_one({'id':p[0],'uploader_id':p[1],
                'published':('true' in q),'created_at':t},**kw)
        elif q.startswith('insert into blbot_items'):
            await c('blbot_items').insert_one({'package_id':p[0],'position':p[1],
                'backup_message_id':p[2],'backup_chat_id':p[3]},**kw)
        elif q.startswith('insert into blbot_ingested'):
            await c('blbot_ingested').insert_one({'source_chat_id':p[0],'source_message_id':p[1],
                'package_id':p[2]},**kw)
        elif q.startswith('insert into blbot_drafts'):
            await c('blbot_drafts').insert_one({'uploader_id':p[0],'package_id':p[1]},**kw)
        elif q.startswith('update blbot_packages set published=true'):
            await c('blbot_packages').update_one({'id':p[0],'uploader_id':p[1]}, {'$set':{'published':True}},**kw)
        elif q.startswith('delete from blbot_drafts'):
            await c('blbot_drafts').delete_one({'uploader_id':p[0]},**kw)
        elif q.startswith('delete from blbot_packages'):
            await c('blbot_packages').delete_one({'id':p[0],'published':False},**kw)
            await c('blbot_items').delete_many({'package_id':p[0]},**kw)
            await c('blbot_ingested').delete_many({'package_id':p[0]},**kw)
        elif q.startswith('insert into blbot_force_chats'):
            await c('blbot_force_chats').update_one({'chat_id':p[0]},
                {'$set':{'name':p[1],'kind':p[2],'join_url':p[3]}},upsert=True,**kw)
        elif q.startswith('delete from blbot_force_chats'):
            r=await c('blbot_force_chats').delete_one({'chat_id':p[0]},**kw)
            return f'DELETE {r.deleted_count}'
        elif q.startswith('insert into blbot_sudo'):
            try:await c('blbot_sudo').insert_one({'user_id':p[0],'added_at':t},**kw)
            except DuplicateKeyError:pass
        elif q.startswith('delete from blbot_sudo'):
            await c('blbot_sudo').delete_one({'user_id':p[0]},**kw)
        elif q.startswith('insert into blbot_admin_input'):
            await c('blbot_admin_input').update_one({'user_id':p[0]},
                {'$set':{'action':p[1],'created_at':t}},upsert=True,**kw)
        elif q.startswith('delete from blbot_admin_input'):
            await c('blbot_admin_input').delete_one({'user_id':p[0]},**kw)
        elif q.startswith('insert into blbot_join_requests'):
            await c('blbot_join_requests').update_one({'chat_id':p[0],'user_id':p[1]},
                {'$set':{'requested_at':p[2]}},upsert=True,**kw)
        elif q.startswith('delete from blbot_join_requests where chat_id='):
            await c('blbot_join_requests').delete_one({'chat_id':p[0],'user_id':p[1]},**kw)
        elif q.startswith('delete from blbot_sent_media'):
            await c('blbot_sent_media').delete_one({'chat_id':p[0],'message_id':p[1]},**kw)
        elif q.startswith('update blbot_sent_media set attempts='):
            await c('blbot_sent_media').update_one({'chat_id':p[0],'message_id':p[1]},
                {'$inc':{'attempts':1},'$set':{'next_try_at':t+timedelta(minutes=5)}},**kw)
        elif q.startswith('delete from blbot_unlocks where created_at<'):
            await c('blbot_unlocks').delete_many({'created_at':{'$lt':t-timedelta(days=30)}},**kw)
        elif q.startswith('delete from blbot_join_requests where requested_at<'):
            await c('blbot_join_requests').delete_many({'requested_at':{'$lt':t-timedelta(hours=25)}},**kw)
        else:
            raise NotImplementedError('Unsupported execute operation: '+q[:120])
        return 'OK 1'
