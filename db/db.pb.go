// Code generated by protoc-gen-gogo.
// source: db.proto
// DO NOT EDIT!

/*
	Package db is a generated protocol buffer package.

	It is generated from these files:
		db.proto

	It has these top-level messages:
		Post
		User
		Kids
		Listing
*/
package db

import proto "github.com/golang/protobuf/proto"
import fmt "fmt"
import math "math"

import io "io"

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = fmt.Errorf
var _ = math.Inf

// This is a compile-time assertion to ensure that this generated file
// is compatible with the proto package it is being compiled against.
// A compilation error at this line likely means your copy of the
// proto package needs to be updated.
const _ = proto.ProtoPackageIsVersion2 // please upgrade the proto package

type Post struct {
	Author int64   `protobuf:"varint,1,opt,name=author,proto3" json:"author,omitempty"`
	Dead   bool    `protobuf:"varint,2,opt,name=dead,proto3" json:"dead,omitempty"`
	Parent int64   `protobuf:"varint,3,opt,name=parent,proto3" json:"parent,omitempty"`
	Url    string  `protobuf:"bytes,4,opt,name=url,proto3" json:"url,omitempty"`
	Title  string  `protobuf:"bytes,5,opt,name=title,proto3" json:"title,omitempty"`
	Text   string  `protobuf:"bytes,7,opt,name=text,proto3" json:"text,omitempty"`
	Parts  []int64 `protobuf:"varint,8,rep,packed,name=parts" json:"parts,omitempty"`
}

func (m *Post) Reset()                    { *m = Post{} }
func (m *Post) String() string            { return proto.CompactTextString(m) }
func (*Post) ProtoMessage()               {}
func (*Post) Descriptor() ([]byte, []int) { return fileDescriptorDb, []int{0} }

func (m *Post) GetAuthor() int64 {
	if m != nil {
		return m.Author
	}
	return 0
}

func (m *Post) GetDead() bool {
	if m != nil {
		return m.Dead
	}
	return false
}

func (m *Post) GetParent() int64 {
	if m != nil {
		return m.Parent
	}
	return 0
}

func (m *Post) GetUrl() string {
	if m != nil {
		return m.Url
	}
	return ""
}

func (m *Post) GetTitle() string {
	if m != nil {
		return m.Title
	}
	return ""
}

func (m *Post) GetText() string {
	if m != nil {
		return m.Text
	}
	return ""
}

func (m *Post) GetParts() []int64 {
	if m != nil {
		return m.Parts
	}
	return nil
}

type User struct {
	Name  string `protobuf:"bytes,1,opt,name=name,proto3" json:"name,omitempty"`
	About string `protobuf:"bytes,2,opt,name=about,proto3" json:"about,omitempty"`
}

func (m *User) Reset()                    { *m = User{} }
func (m *User) String() string            { return proto.CompactTextString(m) }
func (*User) ProtoMessage()               {}
func (*User) Descriptor() ([]byte, []int) { return fileDescriptorDb, []int{1} }

func (m *User) GetName() string {
	if m != nil {
		return m.Name
	}
	return ""
}

func (m *User) GetAbout() string {
	if m != nil {
		return m.About
	}
	return ""
}

type Kids struct {
	Kids []int64 `protobuf:"varint,1,rep,packed,name=kids" json:"kids,omitempty"`
}

func (m *Kids) Reset()                    { *m = Kids{} }
func (m *Kids) String() string            { return proto.CompactTextString(m) }
func (*Kids) ProtoMessage()               {}
func (*Kids) Descriptor() ([]byte, []int) { return fileDescriptorDb, []int{2} }

func (m *Kids) GetKids() []int64 {
	if m != nil {
		return m.Kids
	}
	return nil
}

type Listing struct {
	Objects []int64 `protobuf:"varint,1,rep,packed,name=objects" json:"objects,omitempty"`
}

func (m *Listing) Reset()                    { *m = Listing{} }
func (m *Listing) String() string            { return proto.CompactTextString(m) }
func (*Listing) ProtoMessage()               {}
func (*Listing) Descriptor() ([]byte, []int) { return fileDescriptorDb, []int{3} }

func (m *Listing) GetObjects() []int64 {
	if m != nil {
		return m.Objects
	}
	return nil
}

func init() {
	proto.RegisterType((*Post)(nil), "db.post")
	proto.RegisterType((*User)(nil), "db.user")
	proto.RegisterType((*Kids)(nil), "db.kids")
	proto.RegisterType((*Listing)(nil), "db.listing")
}
func (m *Post) Marshal() (dAtA []byte, err error) {
	size := m.Size()
	dAtA = make([]byte, size)
	n, err := m.MarshalTo(dAtA)
	if err != nil {
		return nil, err
	}
	return dAtA[:n], nil
}

func (m *Post) MarshalTo(dAtA []byte) (int, error) {
	var i int
	_ = i
	var l int
	_ = l
	if m.Author != 0 {
		dAtA[i] = 0x8
		i++
		i = encodeVarintDb(dAtA, i, uint64(m.Author))
	}
	if m.Dead {
		dAtA[i] = 0x10
		i++
		if m.Dead {
			dAtA[i] = 1
		} else {
			dAtA[i] = 0
		}
		i++
	}
	if m.Parent != 0 {
		dAtA[i] = 0x18
		i++
		i = encodeVarintDb(dAtA, i, uint64(m.Parent))
	}
	if len(m.Url) > 0 {
		dAtA[i] = 0x22
		i++
		i = encodeVarintDb(dAtA, i, uint64(len(m.Url)))
		i += copy(dAtA[i:], m.Url)
	}
	if len(m.Title) > 0 {
		dAtA[i] = 0x2a
		i++
		i = encodeVarintDb(dAtA, i, uint64(len(m.Title)))
		i += copy(dAtA[i:], m.Title)
	}
	if len(m.Text) > 0 {
		dAtA[i] = 0x3a
		i++
		i = encodeVarintDb(dAtA, i, uint64(len(m.Text)))
		i += copy(dAtA[i:], m.Text)
	}
	if len(m.Parts) > 0 {
		dAtA2 := make([]byte, len(m.Parts)*10)
		var j1 int
		for _, num1 := range m.Parts {
			num := uint64(num1)
			for num >= 1<<7 {
				dAtA2[j1] = uint8(uint64(num)&0x7f | 0x80)
				num >>= 7
				j1++
			}
			dAtA2[j1] = uint8(num)
			j1++
		}
		dAtA[i] = 0x42
		i++
		i = encodeVarintDb(dAtA, i, uint64(j1))
		i += copy(dAtA[i:], dAtA2[:j1])
	}
	return i, nil
}

func (m *User) Marshal() (dAtA []byte, err error) {
	size := m.Size()
	dAtA = make([]byte, size)
	n, err := m.MarshalTo(dAtA)
	if err != nil {
		return nil, err
	}
	return dAtA[:n], nil
}

func (m *User) MarshalTo(dAtA []byte) (int, error) {
	var i int
	_ = i
	var l int
	_ = l
	if len(m.Name) > 0 {
		dAtA[i] = 0xa
		i++
		i = encodeVarintDb(dAtA, i, uint64(len(m.Name)))
		i += copy(dAtA[i:], m.Name)
	}
	if len(m.About) > 0 {
		dAtA[i] = 0x12
		i++
		i = encodeVarintDb(dAtA, i, uint64(len(m.About)))
		i += copy(dAtA[i:], m.About)
	}
	return i, nil
}

func (m *Kids) Marshal() (dAtA []byte, err error) {
	size := m.Size()
	dAtA = make([]byte, size)
	n, err := m.MarshalTo(dAtA)
	if err != nil {
		return nil, err
	}
	return dAtA[:n], nil
}

func (m *Kids) MarshalTo(dAtA []byte) (int, error) {
	var i int
	_ = i
	var l int
	_ = l
	if len(m.Kids) > 0 {
		dAtA4 := make([]byte, len(m.Kids)*10)
		var j3 int
		for _, num1 := range m.Kids {
			num := uint64(num1)
			for num >= 1<<7 {
				dAtA4[j3] = uint8(uint64(num)&0x7f | 0x80)
				num >>= 7
				j3++
			}
			dAtA4[j3] = uint8(num)
			j3++
		}
		dAtA[i] = 0xa
		i++
		i = encodeVarintDb(dAtA, i, uint64(j3))
		i += copy(dAtA[i:], dAtA4[:j3])
	}
	return i, nil
}

func (m *Listing) Marshal() (dAtA []byte, err error) {
	size := m.Size()
	dAtA = make([]byte, size)
	n, err := m.MarshalTo(dAtA)
	if err != nil {
		return nil, err
	}
	return dAtA[:n], nil
}

func (m *Listing) MarshalTo(dAtA []byte) (int, error) {
	var i int
	_ = i
	var l int
	_ = l
	if len(m.Objects) > 0 {
		dAtA6 := make([]byte, len(m.Objects)*10)
		var j5 int
		for _, num1 := range m.Objects {
			num := uint64(num1)
			for num >= 1<<7 {
				dAtA6[j5] = uint8(uint64(num)&0x7f | 0x80)
				num >>= 7
				j5++
			}
			dAtA6[j5] = uint8(num)
			j5++
		}
		dAtA[i] = 0xa
		i++
		i = encodeVarintDb(dAtA, i, uint64(j5))
		i += copy(dAtA[i:], dAtA6[:j5])
	}
	return i, nil
}

func encodeFixed64Db(dAtA []byte, offset int, v uint64) int {
	dAtA[offset] = uint8(v)
	dAtA[offset+1] = uint8(v >> 8)
	dAtA[offset+2] = uint8(v >> 16)
	dAtA[offset+3] = uint8(v >> 24)
	dAtA[offset+4] = uint8(v >> 32)
	dAtA[offset+5] = uint8(v >> 40)
	dAtA[offset+6] = uint8(v >> 48)
	dAtA[offset+7] = uint8(v >> 56)
	return offset + 8
}
func encodeFixed32Db(dAtA []byte, offset int, v uint32) int {
	dAtA[offset] = uint8(v)
	dAtA[offset+1] = uint8(v >> 8)
	dAtA[offset+2] = uint8(v >> 16)
	dAtA[offset+3] = uint8(v >> 24)
	return offset + 4
}
func encodeVarintDb(dAtA []byte, offset int, v uint64) int {
	for v >= 1<<7 {
		dAtA[offset] = uint8(v&0x7f | 0x80)
		v >>= 7
		offset++
	}
	dAtA[offset] = uint8(v)
	return offset + 1
}
func (m *Post) Size() (n int) {
	var l int
	_ = l
	if m.Author != 0 {
		n += 1 + sovDb(uint64(m.Author))
	}
	if m.Dead {
		n += 2
	}
	if m.Parent != 0 {
		n += 1 + sovDb(uint64(m.Parent))
	}
	l = len(m.Url)
	if l > 0 {
		n += 1 + l + sovDb(uint64(l))
	}
	l = len(m.Title)
	if l > 0 {
		n += 1 + l + sovDb(uint64(l))
	}
	l = len(m.Text)
	if l > 0 {
		n += 1 + l + sovDb(uint64(l))
	}
	if len(m.Parts) > 0 {
		l = 0
		for _, e := range m.Parts {
			l += sovDb(uint64(e))
		}
		n += 1 + sovDb(uint64(l)) + l
	}
	return n
}

func (m *User) Size() (n int) {
	var l int
	_ = l
	l = len(m.Name)
	if l > 0 {
		n += 1 + l + sovDb(uint64(l))
	}
	l = len(m.About)
	if l > 0 {
		n += 1 + l + sovDb(uint64(l))
	}
	return n
}

func (m *Kids) Size() (n int) {
	var l int
	_ = l
	if len(m.Kids) > 0 {
		l = 0
		for _, e := range m.Kids {
			l += sovDb(uint64(e))
		}
		n += 1 + sovDb(uint64(l)) + l
	}
	return n
}

func (m *Listing) Size() (n int) {
	var l int
	_ = l
	if len(m.Objects) > 0 {
		l = 0
		for _, e := range m.Objects {
			l += sovDb(uint64(e))
		}
		n += 1 + sovDb(uint64(l)) + l
	}
	return n
}

func sovDb(x uint64) (n int) {
	for {
		n++
		x >>= 7
		if x == 0 {
			break
		}
	}
	return n
}
func sozDb(x uint64) (n int) {
	return sovDb(uint64((x << 1) ^ uint64((int64(x) >> 63))))
}
func (m *Post) Unmarshal(dAtA []byte) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowDb
			}
			if iNdEx >= l {
				return io.ErrUnexpectedEOF
			}
			b := dAtA[iNdEx]
			iNdEx++
			wire |= (uint64(b) & 0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		wireType := int(wire & 0x7)
		if wireType == 4 {
			return fmt.Errorf("proto: post: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: post: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field Author", wireType)
			}
			m.Author = 0
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowDb
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				m.Author |= (int64(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
		case 2:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field Dead", wireType)
			}
			var v int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowDb
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				v |= (int(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			m.Dead = bool(v != 0)
		case 3:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field Parent", wireType)
			}
			m.Parent = 0
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowDb
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				m.Parent |= (int64(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
		case 4:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Url", wireType)
			}
			var stringLen uint64
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowDb
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				stringLen |= (uint64(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			intStringLen := int(stringLen)
			if intStringLen < 0 {
				return ErrInvalidLengthDb
			}
			postIndex := iNdEx + intStringLen
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.Url = string(dAtA[iNdEx:postIndex])
			iNdEx = postIndex
		case 5:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Title", wireType)
			}
			var stringLen uint64
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowDb
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				stringLen |= (uint64(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			intStringLen := int(stringLen)
			if intStringLen < 0 {
				return ErrInvalidLengthDb
			}
			postIndex := iNdEx + intStringLen
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.Title = string(dAtA[iNdEx:postIndex])
			iNdEx = postIndex
		case 7:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Text", wireType)
			}
			var stringLen uint64
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowDb
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				stringLen |= (uint64(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			intStringLen := int(stringLen)
			if intStringLen < 0 {
				return ErrInvalidLengthDb
			}
			postIndex := iNdEx + intStringLen
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.Text = string(dAtA[iNdEx:postIndex])
			iNdEx = postIndex
		case 8:
			if wireType == 0 {
				var v int64
				for shift := uint(0); ; shift += 7 {
					if shift >= 64 {
						return ErrIntOverflowDb
					}
					if iNdEx >= l {
						return io.ErrUnexpectedEOF
					}
					b := dAtA[iNdEx]
					iNdEx++
					v |= (int64(b) & 0x7F) << shift
					if b < 0x80 {
						break
					}
				}
				m.Parts = append(m.Parts, v)
			} else if wireType == 2 {
				var packedLen int
				for shift := uint(0); ; shift += 7 {
					if shift >= 64 {
						return ErrIntOverflowDb
					}
					if iNdEx >= l {
						return io.ErrUnexpectedEOF
					}
					b := dAtA[iNdEx]
					iNdEx++
					packedLen |= (int(b) & 0x7F) << shift
					if b < 0x80 {
						break
					}
				}
				if packedLen < 0 {
					return ErrInvalidLengthDb
				}
				postIndex := iNdEx + packedLen
				if postIndex > l {
					return io.ErrUnexpectedEOF
				}
				for iNdEx < postIndex {
					var v int64
					for shift := uint(0); ; shift += 7 {
						if shift >= 64 {
							return ErrIntOverflowDb
						}
						if iNdEx >= l {
							return io.ErrUnexpectedEOF
						}
						b := dAtA[iNdEx]
						iNdEx++
						v |= (int64(b) & 0x7F) << shift
						if b < 0x80 {
							break
						}
					}
					m.Parts = append(m.Parts, v)
				}
			} else {
				return fmt.Errorf("proto: wrong wireType = %d for field Parts", wireType)
			}
		default:
			iNdEx = preIndex
			skippy, err := skipDb(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if skippy < 0 {
				return ErrInvalidLengthDb
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			iNdEx += skippy
		}
	}

	if iNdEx > l {
		return io.ErrUnexpectedEOF
	}
	return nil
}
func (m *User) Unmarshal(dAtA []byte) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowDb
			}
			if iNdEx >= l {
				return io.ErrUnexpectedEOF
			}
			b := dAtA[iNdEx]
			iNdEx++
			wire |= (uint64(b) & 0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		wireType := int(wire & 0x7)
		if wireType == 4 {
			return fmt.Errorf("proto: user: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: user: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Name", wireType)
			}
			var stringLen uint64
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowDb
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				stringLen |= (uint64(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			intStringLen := int(stringLen)
			if intStringLen < 0 {
				return ErrInvalidLengthDb
			}
			postIndex := iNdEx + intStringLen
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.Name = string(dAtA[iNdEx:postIndex])
			iNdEx = postIndex
		case 2:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field About", wireType)
			}
			var stringLen uint64
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowDb
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				stringLen |= (uint64(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			intStringLen := int(stringLen)
			if intStringLen < 0 {
				return ErrInvalidLengthDb
			}
			postIndex := iNdEx + intStringLen
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.About = string(dAtA[iNdEx:postIndex])
			iNdEx = postIndex
		default:
			iNdEx = preIndex
			skippy, err := skipDb(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if skippy < 0 {
				return ErrInvalidLengthDb
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			iNdEx += skippy
		}
	}

	if iNdEx > l {
		return io.ErrUnexpectedEOF
	}
	return nil
}
func (m *Kids) Unmarshal(dAtA []byte) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowDb
			}
			if iNdEx >= l {
				return io.ErrUnexpectedEOF
			}
			b := dAtA[iNdEx]
			iNdEx++
			wire |= (uint64(b) & 0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		wireType := int(wire & 0x7)
		if wireType == 4 {
			return fmt.Errorf("proto: kids: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: kids: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType == 0 {
				var v int64
				for shift := uint(0); ; shift += 7 {
					if shift >= 64 {
						return ErrIntOverflowDb
					}
					if iNdEx >= l {
						return io.ErrUnexpectedEOF
					}
					b := dAtA[iNdEx]
					iNdEx++
					v |= (int64(b) & 0x7F) << shift
					if b < 0x80 {
						break
					}
				}
				m.Kids = append(m.Kids, v)
			} else if wireType == 2 {
				var packedLen int
				for shift := uint(0); ; shift += 7 {
					if shift >= 64 {
						return ErrIntOverflowDb
					}
					if iNdEx >= l {
						return io.ErrUnexpectedEOF
					}
					b := dAtA[iNdEx]
					iNdEx++
					packedLen |= (int(b) & 0x7F) << shift
					if b < 0x80 {
						break
					}
				}
				if packedLen < 0 {
					return ErrInvalidLengthDb
				}
				postIndex := iNdEx + packedLen
				if postIndex > l {
					return io.ErrUnexpectedEOF
				}
				for iNdEx < postIndex {
					var v int64
					for shift := uint(0); ; shift += 7 {
						if shift >= 64 {
							return ErrIntOverflowDb
						}
						if iNdEx >= l {
							return io.ErrUnexpectedEOF
						}
						b := dAtA[iNdEx]
						iNdEx++
						v |= (int64(b) & 0x7F) << shift
						if b < 0x80 {
							break
						}
					}
					m.Kids = append(m.Kids, v)
				}
			} else {
				return fmt.Errorf("proto: wrong wireType = %d for field Kids", wireType)
			}
		default:
			iNdEx = preIndex
			skippy, err := skipDb(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if skippy < 0 {
				return ErrInvalidLengthDb
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			iNdEx += skippy
		}
	}

	if iNdEx > l {
		return io.ErrUnexpectedEOF
	}
	return nil
}
func (m *Listing) Unmarshal(dAtA []byte) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowDb
			}
			if iNdEx >= l {
				return io.ErrUnexpectedEOF
			}
			b := dAtA[iNdEx]
			iNdEx++
			wire |= (uint64(b) & 0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		wireType := int(wire & 0x7)
		if wireType == 4 {
			return fmt.Errorf("proto: listing: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: listing: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType == 0 {
				var v int64
				for shift := uint(0); ; shift += 7 {
					if shift >= 64 {
						return ErrIntOverflowDb
					}
					if iNdEx >= l {
						return io.ErrUnexpectedEOF
					}
					b := dAtA[iNdEx]
					iNdEx++
					v |= (int64(b) & 0x7F) << shift
					if b < 0x80 {
						break
					}
				}
				m.Objects = append(m.Objects, v)
			} else if wireType == 2 {
				var packedLen int
				for shift := uint(0); ; shift += 7 {
					if shift >= 64 {
						return ErrIntOverflowDb
					}
					if iNdEx >= l {
						return io.ErrUnexpectedEOF
					}
					b := dAtA[iNdEx]
					iNdEx++
					packedLen |= (int(b) & 0x7F) << shift
					if b < 0x80 {
						break
					}
				}
				if packedLen < 0 {
					return ErrInvalidLengthDb
				}
				postIndex := iNdEx + packedLen
				if postIndex > l {
					return io.ErrUnexpectedEOF
				}
				for iNdEx < postIndex {
					var v int64
					for shift := uint(0); ; shift += 7 {
						if shift >= 64 {
							return ErrIntOverflowDb
						}
						if iNdEx >= l {
							return io.ErrUnexpectedEOF
						}
						b := dAtA[iNdEx]
						iNdEx++
						v |= (int64(b) & 0x7F) << shift
						if b < 0x80 {
							break
						}
					}
					m.Objects = append(m.Objects, v)
				}
			} else {
				return fmt.Errorf("proto: wrong wireType = %d for field Objects", wireType)
			}
		default:
			iNdEx = preIndex
			skippy, err := skipDb(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if skippy < 0 {
				return ErrInvalidLengthDb
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			iNdEx += skippy
		}
	}

	if iNdEx > l {
		return io.ErrUnexpectedEOF
	}
	return nil
}
func skipDb(dAtA []byte) (n int, err error) {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return 0, ErrIntOverflowDb
			}
			if iNdEx >= l {
				return 0, io.ErrUnexpectedEOF
			}
			b := dAtA[iNdEx]
			iNdEx++
			wire |= (uint64(b) & 0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		wireType := int(wire & 0x7)
		switch wireType {
		case 0:
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return 0, ErrIntOverflowDb
				}
				if iNdEx >= l {
					return 0, io.ErrUnexpectedEOF
				}
				iNdEx++
				if dAtA[iNdEx-1] < 0x80 {
					break
				}
			}
			return iNdEx, nil
		case 1:
			iNdEx += 8
			return iNdEx, nil
		case 2:
			var length int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return 0, ErrIntOverflowDb
				}
				if iNdEx >= l {
					return 0, io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				length |= (int(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			iNdEx += length
			if length < 0 {
				return 0, ErrInvalidLengthDb
			}
			return iNdEx, nil
		case 3:
			for {
				var innerWire uint64
				var start int = iNdEx
				for shift := uint(0); ; shift += 7 {
					if shift >= 64 {
						return 0, ErrIntOverflowDb
					}
					if iNdEx >= l {
						return 0, io.ErrUnexpectedEOF
					}
					b := dAtA[iNdEx]
					iNdEx++
					innerWire |= (uint64(b) & 0x7F) << shift
					if b < 0x80 {
						break
					}
				}
				innerWireType := int(innerWire & 0x7)
				if innerWireType == 4 {
					break
				}
				next, err := skipDb(dAtA[start:])
				if err != nil {
					return 0, err
				}
				iNdEx = start + next
			}
			return iNdEx, nil
		case 4:
			return iNdEx, nil
		case 5:
			iNdEx += 4
			return iNdEx, nil
		default:
			return 0, fmt.Errorf("proto: illegal wireType %d", wireType)
		}
	}
	panic("unreachable")
}

var (
	ErrInvalidLengthDb = fmt.Errorf("proto: negative length found during unmarshaling")
	ErrIntOverflowDb   = fmt.Errorf("proto: integer overflow")
)

func init() { proto.RegisterFile("db.proto", fileDescriptorDb) }

var fileDescriptorDb = []byte{
	// 239 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0x34, 0x90, 0x41, 0x4a, 0xc5, 0x30,
	0x10, 0x86, 0xcd, 0x4b, 0xde, 0x6b, 0x3b, 0xab, 0x47, 0x10, 0x09, 0x2e, 0x4a, 0xa9, 0x9b, 0xae,
	0x44, 0xf0, 0x06, 0x1e, 0x21, 0x37, 0x48, 0x4c, 0xd0, 0x6a, 0x6d, 0x4a, 0x32, 0x05, 0x8f, 0xe2,
	0xc2, 0x03, 0xb9, 0xf4, 0x08, 0x52, 0x2f, 0x22, 0x99, 0xd8, 0x55, 0xbe, 0x6f, 0xf8, 0x67, 0xf8,
	0x09, 0xd4, 0xce, 0xde, 0x2e, 0x31, 0x60, 0x90, 0x07, 0x67, 0xfb, 0x4f, 0x06, 0x62, 0x09, 0x09,
	0xe5, 0x15, 0x9c, 0xcc, 0x8a, 0xcf, 0x21, 0x2a, 0xd6, 0xb1, 0x81, 0xeb, 0x7f, 0x93, 0x12, 0x84,
	0xf3, 0xc6, 0xa9, 0x43, 0xc7, 0x86, 0x5a, 0x13, 0xe7, 0xec, 0x62, 0xa2, 0x9f, 0x51, 0xf1, 0x92,
	0x2d, 0x26, 0xcf, 0xc0, 0xd7, 0x38, 0x29, 0xd1, 0xb1, 0xa1, 0xd1, 0x19, 0xe5, 0x25, 0x1c, 0x71,
	0xc4, 0xc9, 0xab, 0x23, 0xcd, 0x8a, 0xe4, 0x9b, 0xe8, 0xdf, 0x51, 0x55, 0x34, 0x24, 0xce, 0xc9,
	0xc5, 0x44, 0x4c, 0xaa, 0xee, 0xf8, 0xc0, 0x75, 0x91, 0xfe, 0x0e, 0xc4, 0x9a, 0x3c, 0xb5, 0x98,
	0xcd, 0x9b, 0xa7, 0x6e, 0x8d, 0x26, 0xce, 0x1b, 0xc6, 0x86, 0x15, 0xa9, 0x5a, 0xa3, 0x8b, 0xf4,
	0xd7, 0x20, 0x5e, 0x47, 0x97, 0xf2, 0x46, 0x7e, 0x15, 0xa3, 0x73, 0xc4, 0xfd, 0x0d, 0x54, 0xd3,
	0x98, 0x70, 0x9c, 0x9f, 0xa4, 0x82, 0x2a, 0xd8, 0x17, 0xff, 0x88, 0x7b, 0x62, 0xd7, 0x87, 0xf3,
	0xd7, 0xd6, 0xb2, 0xef, 0xad, 0x65, 0x3f, 0x5b, 0xcb, 0x3e, 0x7e, 0xdb, 0x0b, 0x7b, 0xa2, 0xef,
	0xba, 0xff, 0x0b, 0x00, 0x00, 0xff, 0xff, 0x38, 0x0c, 0x05, 0x83, 0x3a, 0x01, 0x00, 0x00,
}
