# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: workerServer.proto
# Protobuf Python Version: 4.25.1
"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import symbol_database as _symbol_database
from google.protobuf.internal import builder as _builder
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()




DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n\x12workerServer.proto\"6\n\x0eMigrationInput\x12\n\n\x02ip\x18\x01 \x01(\t\x12\x0c\n\x04port\x18\x02 \x01(\t\x12\n\n\x02id\x18\x03 \x01(\t\"!\n\x0fMigrationOutput\x12\x0e\n\x06status\x18\x01 \x01(\x08\"0\n\x0eStoreDataInput\x12\n\n\x02id\x18\x01 \x01(\t\x12\x12\n\nimage_data\x18\x02 \x01(\x0c\"!\n\x0fStoreDataOutput\x12\x0e\n\x06status\x18\x01 \x01(\x08\x32j\n\x06Worker\x12.\n\x07migrate\x12\x0f.MigrationInput\x1a\x10.MigrationOutput\"\x00\x12\x30\n\tstoreDate\x12\x0f.StoreDataInput\x1a\x10.StoreDataOutput\"\x00\x62\x06proto3')

_globals = globals()
_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, _globals)
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, 'workerServer_pb2', _globals)
if _descriptor._USE_C_DESCRIPTORS == False:
  DESCRIPTOR._options = None
  _globals['_MIGRATIONINPUT']._serialized_start=22
  _globals['_MIGRATIONINPUT']._serialized_end=76
  _globals['_MIGRATIONOUTPUT']._serialized_start=78
  _globals['_MIGRATIONOUTPUT']._serialized_end=111
  _globals['_STOREDATAINPUT']._serialized_start=113
  _globals['_STOREDATAINPUT']._serialized_end=161
  _globals['_STOREDATAOUTPUT']._serialized_start=163
  _globals['_STOREDATAOUTPUT']._serialized_end=196
  _globals['_WORKER']._serialized_start=198
  _globals['_WORKER']._serialized_end=304
# @@protoc_insertion_point(module_scope)