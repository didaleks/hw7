// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.28.0
// 	protoc        v3.19.4
// source: service.proto

package main

import (
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	reflect "reflect"
	sync "sync"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

type Event struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Timestamp int64  `protobuf:"varint,1,opt,name=timestamp,proto3" json:"timestamp,omitempty"`
	Consumer  string `protobuf:"bytes,2,opt,name=consumer,proto3" json:"consumer,omitempty"`
	Method    string `protobuf:"bytes,3,opt,name=method,proto3" json:"method,omitempty"`
	Host      string `protobuf:"bytes,4,opt,name=host,proto3" json:"host,omitempty"`
}

func (x *Event) Reset() {
	*x = Event{}
	if protoimpl.UnsafeEnabled {
		mi := &file_service_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Event) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Event) ProtoMessage() {}

func (x *Event) ProtoReflect() protoreflect.Message {
	mi := &file_service_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Event.ProtoReflect.Descriptor instead.
func (*Event) Descriptor() ([]byte, []int) {
	return file_service_proto_rawDescGZIP(), []int{0}
}

func (x *Event) GetTimestamp() int64 {
	if x != nil {
		return x.Timestamp
	}
	return 0
}

func (x *Event) GetConsumer() string {
	if x != nil {
		return x.Consumer
	}
	return ""
}

func (x *Event) GetMethod() string {
	if x != nil {
		return x.Method
	}
	return ""
}

func (x *Event) GetHost() string {
	if x != nil {
		return x.Host
	}
	return ""
}

type Stat struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Timestamp  int64             `protobuf:"varint,1,opt,name=timestamp,proto3" json:"timestamp,omitempty"`
	ByMethod   map[string]uint64 `protobuf:"bytes,2,rep,name=by_method,json=byMethod,proto3" json:"by_method,omitempty" protobuf_key:"bytes,1,opt,name=key,proto3" protobuf_val:"varint,2,opt,name=value,proto3"`
	ByConsumer map[string]uint64 `protobuf:"bytes,3,rep,name=by_consumer,json=byConsumer,proto3" json:"by_consumer,omitempty" protobuf_key:"bytes,1,opt,name=key,proto3" protobuf_val:"varint,2,opt,name=value,proto3"`
}

func (x *Stat) Reset() {
	*x = Stat{}
	if protoimpl.UnsafeEnabled {
		mi := &file_service_proto_msgTypes[1]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Stat) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Stat) ProtoMessage() {}

func (x *Stat) ProtoReflect() protoreflect.Message {
	mi := &file_service_proto_msgTypes[1]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Stat.ProtoReflect.Descriptor instead.
func (*Stat) Descriptor() ([]byte, []int) {
	return file_service_proto_rawDescGZIP(), []int{1}
}

func (x *Stat) GetTimestamp() int64 {
	if x != nil {
		return x.Timestamp
	}
	return 0
}

func (x *Stat) GetByMethod() map[string]uint64 {
	if x != nil {
		return x.ByMethod
	}
	return nil
}

func (x *Stat) GetByConsumer() map[string]uint64 {
	if x != nil {
		return x.ByConsumer
	}
	return nil
}

type StatInterval struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	IntervalSeconds uint64 `protobuf:"varint,1,opt,name=interval_seconds,json=intervalSeconds,proto3" json:"interval_seconds,omitempty"`
}

func (x *StatInterval) Reset() {
	*x = StatInterval{}
	if protoimpl.UnsafeEnabled {
		mi := &file_service_proto_msgTypes[2]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *StatInterval) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*StatInterval) ProtoMessage() {}

func (x *StatInterval) ProtoReflect() protoreflect.Message {
	mi := &file_service_proto_msgTypes[2]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use StatInterval.ProtoReflect.Descriptor instead.
func (*StatInterval) Descriptor() ([]byte, []int) {
	return file_service_proto_rawDescGZIP(), []int{2}
}

func (x *StatInterval) GetIntervalSeconds() uint64 {
	if x != nil {
		return x.IntervalSeconds
	}
	return 0
}

type Nothing struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Dummy bool `protobuf:"varint,1,opt,name=dummy,proto3" json:"dummy,omitempty"`
}

func (x *Nothing) Reset() {
	*x = Nothing{}
	if protoimpl.UnsafeEnabled {
		mi := &file_service_proto_msgTypes[3]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Nothing) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Nothing) ProtoMessage() {}

func (x *Nothing) ProtoReflect() protoreflect.Message {
	mi := &file_service_proto_msgTypes[3]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Nothing.ProtoReflect.Descriptor instead.
func (*Nothing) Descriptor() ([]byte, []int) {
	return file_service_proto_rawDescGZIP(), []int{3}
}

func (x *Nothing) GetDummy() bool {
	if x != nil {
		return x.Dummy
	}
	return false
}

var File_service_proto protoreflect.FileDescriptor

var file_service_proto_rawDesc = []byte{
	0x0a, 0x0d, 0x73, 0x65, 0x72, 0x76, 0x69, 0x63, 0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x12,
	0x04, 0x6d, 0x61, 0x69, 0x6e, 0x22, 0x6d, 0x0a, 0x05, 0x45, 0x76, 0x65, 0x6e, 0x74, 0x12, 0x1c,
	0x0a, 0x09, 0x74, 0x69, 0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d, 0x70, 0x18, 0x01, 0x20, 0x01, 0x28,
	0x03, 0x52, 0x09, 0x74, 0x69, 0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d, 0x70, 0x12, 0x1a, 0x0a, 0x08,
	0x63, 0x6f, 0x6e, 0x73, 0x75, 0x6d, 0x65, 0x72, 0x18, 0x02, 0x20, 0x01, 0x28, 0x09, 0x52, 0x08,
	0x63, 0x6f, 0x6e, 0x73, 0x75, 0x6d, 0x65, 0x72, 0x12, 0x16, 0x0a, 0x06, 0x6d, 0x65, 0x74, 0x68,
	0x6f, 0x64, 0x18, 0x03, 0x20, 0x01, 0x28, 0x09, 0x52, 0x06, 0x6d, 0x65, 0x74, 0x68, 0x6f, 0x64,
	0x12, 0x12, 0x0a, 0x04, 0x68, 0x6f, 0x73, 0x74, 0x18, 0x04, 0x20, 0x01, 0x28, 0x09, 0x52, 0x04,
	0x68, 0x6f, 0x73, 0x74, 0x22, 0x94, 0x02, 0x0a, 0x04, 0x53, 0x74, 0x61, 0x74, 0x12, 0x1c, 0x0a,
	0x09, 0x74, 0x69, 0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d, 0x70, 0x18, 0x01, 0x20, 0x01, 0x28, 0x03,
	0x52, 0x09, 0x74, 0x69, 0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d, 0x70, 0x12, 0x35, 0x0a, 0x09, 0x62,
	0x79, 0x5f, 0x6d, 0x65, 0x74, 0x68, 0x6f, 0x64, 0x18, 0x02, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x18,
	0x2e, 0x6d, 0x61, 0x69, 0x6e, 0x2e, 0x53, 0x74, 0x61, 0x74, 0x2e, 0x42, 0x79, 0x4d, 0x65, 0x74,
	0x68, 0x6f, 0x64, 0x45, 0x6e, 0x74, 0x72, 0x79, 0x52, 0x08, 0x62, 0x79, 0x4d, 0x65, 0x74, 0x68,
	0x6f, 0x64, 0x12, 0x3b, 0x0a, 0x0b, 0x62, 0x79, 0x5f, 0x63, 0x6f, 0x6e, 0x73, 0x75, 0x6d, 0x65,
	0x72, 0x18, 0x03, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x1a, 0x2e, 0x6d, 0x61, 0x69, 0x6e, 0x2e, 0x53,
	0x74, 0x61, 0x74, 0x2e, 0x42, 0x79, 0x43, 0x6f, 0x6e, 0x73, 0x75, 0x6d, 0x65, 0x72, 0x45, 0x6e,
	0x74, 0x72, 0x79, 0x52, 0x0a, 0x62, 0x79, 0x43, 0x6f, 0x6e, 0x73, 0x75, 0x6d, 0x65, 0x72, 0x1a,
	0x3b, 0x0a, 0x0d, 0x42, 0x79, 0x4d, 0x65, 0x74, 0x68, 0x6f, 0x64, 0x45, 0x6e, 0x74, 0x72, 0x79,
	0x12, 0x10, 0x0a, 0x03, 0x6b, 0x65, 0x79, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x03, 0x6b,
	0x65, 0x79, 0x12, 0x14, 0x0a, 0x05, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x18, 0x02, 0x20, 0x01, 0x28,
	0x04, 0x52, 0x05, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x3a, 0x02, 0x38, 0x01, 0x1a, 0x3d, 0x0a, 0x0f,
	0x42, 0x79, 0x43, 0x6f, 0x6e, 0x73, 0x75, 0x6d, 0x65, 0x72, 0x45, 0x6e, 0x74, 0x72, 0x79, 0x12,
	0x10, 0x0a, 0x03, 0x6b, 0x65, 0x79, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x03, 0x6b, 0x65,
	0x79, 0x12, 0x14, 0x0a, 0x05, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x18, 0x02, 0x20, 0x01, 0x28, 0x04,
	0x52, 0x05, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x3a, 0x02, 0x38, 0x01, 0x22, 0x39, 0x0a, 0x0c, 0x53,
	0x74, 0x61, 0x74, 0x49, 0x6e, 0x74, 0x65, 0x72, 0x76, 0x61, 0x6c, 0x12, 0x29, 0x0a, 0x10, 0x69,
	0x6e, 0x74, 0x65, 0x72, 0x76, 0x61, 0x6c, 0x5f, 0x73, 0x65, 0x63, 0x6f, 0x6e, 0x64, 0x73, 0x18,
	0x01, 0x20, 0x01, 0x28, 0x04, 0x52, 0x0f, 0x69, 0x6e, 0x74, 0x65, 0x72, 0x76, 0x61, 0x6c, 0x53,
	0x65, 0x63, 0x6f, 0x6e, 0x64, 0x73, 0x22, 0x1f, 0x0a, 0x07, 0x4e, 0x6f, 0x74, 0x68, 0x69, 0x6e,
	0x67, 0x12, 0x14, 0x0a, 0x05, 0x64, 0x75, 0x6d, 0x6d, 0x79, 0x18, 0x01, 0x20, 0x01, 0x28, 0x08,
	0x52, 0x05, 0x64, 0x75, 0x6d, 0x6d, 0x79, 0x32, 0x64, 0x0a, 0x05, 0x41, 0x64, 0x6d, 0x69, 0x6e,
	0x12, 0x29, 0x0a, 0x07, 0x4c, 0x6f, 0x67, 0x67, 0x69, 0x6e, 0x67, 0x12, 0x0d, 0x2e, 0x6d, 0x61,
	0x69, 0x6e, 0x2e, 0x4e, 0x6f, 0x74, 0x68, 0x69, 0x6e, 0x67, 0x1a, 0x0b, 0x2e, 0x6d, 0x61, 0x69,
	0x6e, 0x2e, 0x45, 0x76, 0x65, 0x6e, 0x74, 0x22, 0x00, 0x30, 0x01, 0x12, 0x30, 0x0a, 0x0a, 0x53,
	0x74, 0x61, 0x74, 0x69, 0x73, 0x74, 0x69, 0x63, 0x73, 0x12, 0x12, 0x2e, 0x6d, 0x61, 0x69, 0x6e,
	0x2e, 0x53, 0x74, 0x61, 0x74, 0x49, 0x6e, 0x74, 0x65, 0x72, 0x76, 0x61, 0x6c, 0x1a, 0x0a, 0x2e,
	0x6d, 0x61, 0x69, 0x6e, 0x2e, 0x53, 0x74, 0x61, 0x74, 0x22, 0x00, 0x30, 0x01, 0x32, 0x7d, 0x0a,
	0x03, 0x42, 0x69, 0x7a, 0x12, 0x27, 0x0a, 0x05, 0x43, 0x68, 0x65, 0x63, 0x6b, 0x12, 0x0d, 0x2e,
	0x6d, 0x61, 0x69, 0x6e, 0x2e, 0x4e, 0x6f, 0x74, 0x68, 0x69, 0x6e, 0x67, 0x1a, 0x0d, 0x2e, 0x6d,
	0x61, 0x69, 0x6e, 0x2e, 0x4e, 0x6f, 0x74, 0x68, 0x69, 0x6e, 0x67, 0x22, 0x00, 0x12, 0x25, 0x0a,
	0x03, 0x41, 0x64, 0x64, 0x12, 0x0d, 0x2e, 0x6d, 0x61, 0x69, 0x6e, 0x2e, 0x4e, 0x6f, 0x74, 0x68,
	0x69, 0x6e, 0x67, 0x1a, 0x0d, 0x2e, 0x6d, 0x61, 0x69, 0x6e, 0x2e, 0x4e, 0x6f, 0x74, 0x68, 0x69,
	0x6e, 0x67, 0x22, 0x00, 0x12, 0x26, 0x0a, 0x04, 0x54, 0x65, 0x73, 0x74, 0x12, 0x0d, 0x2e, 0x6d,
	0x61, 0x69, 0x6e, 0x2e, 0x4e, 0x6f, 0x74, 0x68, 0x69, 0x6e, 0x67, 0x1a, 0x0d, 0x2e, 0x6d, 0x61,
	0x69, 0x6e, 0x2e, 0x4e, 0x6f, 0x74, 0x68, 0x69, 0x6e, 0x67, 0x22, 0x00, 0x42, 0x08, 0x5a, 0x06,
	0x2e, 0x3b, 0x6d, 0x61, 0x69, 0x6e, 0x62, 0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_service_proto_rawDescOnce sync.Once
	file_service_proto_rawDescData = file_service_proto_rawDesc
)

func file_service_proto_rawDescGZIP() []byte {
	file_service_proto_rawDescOnce.Do(func() {
		file_service_proto_rawDescData = protoimpl.X.CompressGZIP(file_service_proto_rawDescData)
	})
	return file_service_proto_rawDescData
}

var file_service_proto_msgTypes = make([]protoimpl.MessageInfo, 6)
var file_service_proto_goTypes = []interface{}{
	(*Event)(nil),        // 0: main.Event
	(*Stat)(nil),         // 1: main.Stat
	(*StatInterval)(nil), // 2: main.StatInterval
	(*Nothing)(nil),      // 3: main.Nothing
	nil,                  // 4: main.Stat.ByMethodEntry
	nil,                  // 5: main.Stat.ByConsumerEntry
}
var file_service_proto_depIdxs = []int32{
	4, // 0: main.Stat.by_method:type_name -> main.Stat.ByMethodEntry
	5, // 1: main.Stat.by_consumer:type_name -> main.Stat.ByConsumerEntry
	3, // 2: main.Admin.Logging:input_type -> main.Nothing
	2, // 3: main.Admin.Statistics:input_type -> main.StatInterval
	3, // 4: main.Biz.Check:input_type -> main.Nothing
	3, // 5: main.Biz.Add:input_type -> main.Nothing
	3, // 6: main.Biz.Test:input_type -> main.Nothing
	0, // 7: main.Admin.Logging:output_type -> main.Event
	1, // 8: main.Admin.Statistics:output_type -> main.Stat
	3, // 9: main.Biz.Check:output_type -> main.Nothing
	3, // 10: main.Biz.Add:output_type -> main.Nothing
	3, // 11: main.Biz.Test:output_type -> main.Nothing
	7, // [7:12] is the sub-list for method output_type
	2, // [2:7] is the sub-list for method input_type
	2, // [2:2] is the sub-list for extension type_name
	2, // [2:2] is the sub-list for extension extendee
	0, // [0:2] is the sub-list for field type_name
}

func init() { file_service_proto_init() }
func file_service_proto_init() {
	if File_service_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_service_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*Event); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_service_proto_msgTypes[1].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*Stat); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_service_proto_msgTypes[2].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*StatInterval); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_service_proto_msgTypes[3].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*Nothing); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_service_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   6,
			NumExtensions: 0,
			NumServices:   2,
		},
		GoTypes:           file_service_proto_goTypes,
		DependencyIndexes: file_service_proto_depIdxs,
		MessageInfos:      file_service_proto_msgTypes,
	}.Build()
	File_service_proto = out.File
	file_service_proto_rawDesc = nil
	file_service_proto_goTypes = nil
	file_service_proto_depIdxs = nil
}
