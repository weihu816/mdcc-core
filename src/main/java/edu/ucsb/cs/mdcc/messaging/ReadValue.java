/**
 * Autogenerated by Thrift Compiler (0.9.1)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
package edu.ucsb.cs.mdcc.messaging;

import org.apache.thrift.scheme.IScheme;
import org.apache.thrift.scheme.SchemeFactory;
import org.apache.thrift.scheme.StandardScheme;

import org.apache.thrift.scheme.TupleScheme;
import org.apache.thrift.protocol.TTupleProtocol;
import org.apache.thrift.protocol.TProtocolException;
import org.apache.thrift.EncodingUtils;
import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;
import org.apache.thrift.server.AbstractNonblockingServer.*;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.EnumMap;
import java.util.Set;
import java.util.HashSet;
import java.util.EnumSet;
import java.util.Collections;
import java.util.BitSet;
import java.nio.ByteBuffer;
import java.util.Arrays;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReadValue implements org.apache.thrift.TBase<ReadValue, ReadValue._Fields>, java.io.Serializable, Cloneable, Comparable<ReadValue> {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("ReadValue");

  private static final org.apache.thrift.protocol.TField KEY_FIELD_DESC = new org.apache.thrift.protocol.TField("key", org.apache.thrift.protocol.TType.STRING, (short)1);
  private static final org.apache.thrift.protocol.TField VERSION_FIELD_DESC = new org.apache.thrift.protocol.TField("version", org.apache.thrift.protocol.TType.I64, (short)2);
  private static final org.apache.thrift.protocol.TField CLASSIC_END_VERSION_FIELD_DESC = new org.apache.thrift.protocol.TField("classicEndVersion", org.apache.thrift.protocol.TType.I64, (short)3);
  private static final org.apache.thrift.protocol.TField VALUE_FIELD_DESC = new org.apache.thrift.protocol.TField("value", org.apache.thrift.protocol.TType.STRING, (short)4);

  private static final Map<Class<? extends IScheme>, SchemeFactory> schemes = new HashMap<Class<? extends IScheme>, SchemeFactory>();
  static {
    schemes.put(StandardScheme.class, new ReadValueStandardSchemeFactory());
    schemes.put(TupleScheme.class, new ReadValueTupleSchemeFactory());
  }

  public String key; // required
  public long version; // required
  public long classicEndVersion; // required
  public ByteBuffer value; // required

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    KEY((short)1, "key"),
    VERSION((short)2, "version"),
    CLASSIC_END_VERSION((short)3, "classicEndVersion"),
    VALUE((short)4, "value");

    private static final Map<String, _Fields> byName = new HashMap<String, _Fields>();

    static {
      for (_Fields field : EnumSet.allOf(_Fields.class)) {
        byName.put(field.getFieldName(), field);
      }
    }

    /**
     * Find the _Fields constant that matches fieldId, or null if its not found.
     */
    public static _Fields findByThriftId(int fieldId) {
      switch(fieldId) {
        case 1: // KEY
          return KEY;
        case 2: // VERSION
          return VERSION;
        case 3: // CLASSIC_END_VERSION
          return CLASSIC_END_VERSION;
        case 4: // VALUE
          return VALUE;
        default:
          return null;
      }
    }

    /**
     * Find the _Fields constant that matches fieldId, throwing an exception
     * if it is not found.
     */
    public static _Fields findByThriftIdOrThrow(int fieldId) {
      _Fields fields = findByThriftId(fieldId);
      if (fields == null) throw new IllegalArgumentException("Field " + fieldId + " doesn't exist!");
      return fields;
    }

    /**
     * Find the _Fields constant that matches name, or null if its not found.
     */
    public static _Fields findByName(String name) {
      return byName.get(name);
    }

    private final short _thriftId;
    private final String _fieldName;

    _Fields(short thriftId, String fieldName) {
      _thriftId = thriftId;
      _fieldName = fieldName;
    }

    public short getThriftFieldId() {
      return _thriftId;
    }

    public String getFieldName() {
      return _fieldName;
    }
  }

  // isset id assignments
  private static final int __VERSION_ISSET_ID = 0;
  private static final int __CLASSICENDVERSION_ISSET_ID = 1;
  private byte __isset_bitfield = 0;
  public static final Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.KEY, new org.apache.thrift.meta_data.FieldMetaData("key", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING)));
    tmpMap.put(_Fields.VERSION, new org.apache.thrift.meta_data.FieldMetaData("version", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I64)));
    tmpMap.put(_Fields.CLASSIC_END_VERSION, new org.apache.thrift.meta_data.FieldMetaData("classicEndVersion", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I64)));
    tmpMap.put(_Fields.VALUE, new org.apache.thrift.meta_data.FieldMetaData("value", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING        , true)));
    metaDataMap = Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(ReadValue.class, metaDataMap);
  }

  public ReadValue() {
  }

  public ReadValue(
    String key,
    long version,
    long classicEndVersion,
    ByteBuffer value)
  {
    this();
    this.key = key;
    this.version = version;
    setVersionIsSet(true);
    this.classicEndVersion = classicEndVersion;
    setClassicEndVersionIsSet(true);
    this.value = value;
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public ReadValue(ReadValue other) {
    __isset_bitfield = other.__isset_bitfield;
    if (other.isSetKey()) {
      this.key = other.key;
    }
    this.version = other.version;
    this.classicEndVersion = other.classicEndVersion;
    if (other.isSetValue()) {
      this.value = org.apache.thrift.TBaseHelper.copyBinary(other.value);
;
    }
  }

  public ReadValue deepCopy() {
    return new ReadValue(this);
  }

  @Override
  public void clear() {
    this.key = null;
    setVersionIsSet(false);
    this.version = 0;
    setClassicEndVersionIsSet(false);
    this.classicEndVersion = 0;
    this.value = null;
  }

  public String getKey() {
    return this.key;
  }

  public ReadValue setKey(String key) {
    this.key = key;
    return this;
  }

  public void unsetKey() {
    this.key = null;
  }

  /** Returns true if field key is set (has been assigned a value) and false otherwise */
  public boolean isSetKey() {
    return this.key != null;
  }

  public void setKeyIsSet(boolean value) {
    if (!value) {
      this.key = null;
    }
  }

  public long getVersion() {
    return this.version;
  }

  public ReadValue setVersion(long version) {
    this.version = version;
    setVersionIsSet(true);
    return this;
  }

  public void unsetVersion() {
    __isset_bitfield = EncodingUtils.clearBit(__isset_bitfield, __VERSION_ISSET_ID);
  }

  /** Returns true if field version is set (has been assigned a value) and false otherwise */
  public boolean isSetVersion() {
    return EncodingUtils.testBit(__isset_bitfield, __VERSION_ISSET_ID);
  }

  public void setVersionIsSet(boolean value) {
    __isset_bitfield = EncodingUtils.setBit(__isset_bitfield, __VERSION_ISSET_ID, value);
  }

  public long getClassicEndVersion() {
    return this.classicEndVersion;
  }

  public ReadValue setClassicEndVersion(long classicEndVersion) {
    this.classicEndVersion = classicEndVersion;
    setClassicEndVersionIsSet(true);
    return this;
  }

  public void unsetClassicEndVersion() {
    __isset_bitfield = EncodingUtils.clearBit(__isset_bitfield, __CLASSICENDVERSION_ISSET_ID);
  }

  /** Returns true if field classicEndVersion is set (has been assigned a value) and false otherwise */
  public boolean isSetClassicEndVersion() {
    return EncodingUtils.testBit(__isset_bitfield, __CLASSICENDVERSION_ISSET_ID);
  }

  public void setClassicEndVersionIsSet(boolean value) {
    __isset_bitfield = EncodingUtils.setBit(__isset_bitfield, __CLASSICENDVERSION_ISSET_ID, value);
  }

  public byte[] getValue() {
    setValue(org.apache.thrift.TBaseHelper.rightSize(value));
    return value == null ? null : value.array();
  }

  public ByteBuffer bufferForValue() {
    return value;
  }

  public ReadValue setValue(byte[] value) {
    setValue(value == null ? (ByteBuffer)null : ByteBuffer.wrap(value));
    return this;
  }

  public ReadValue setValue(ByteBuffer value) {
    this.value = value;
    return this;
  }

  public void unsetValue() {
    this.value = null;
  }

  /** Returns true if field value is set (has been assigned a value) and false otherwise */
  public boolean isSetValue() {
    return this.value != null;
  }

  public void setValueIsSet(boolean value) {
    if (!value) {
      this.value = null;
    }
  }

  public void setFieldValue(_Fields field, Object value) {
    switch (field) {
    case KEY:
      if (value == null) {
        unsetKey();
      } else {
        setKey((String)value);
      }
      break;

    case VERSION:
      if (value == null) {
        unsetVersion();
      } else {
        setVersion((Long)value);
      }
      break;

    case CLASSIC_END_VERSION:
      if (value == null) {
        unsetClassicEndVersion();
      } else {
        setClassicEndVersion((Long)value);
      }
      break;

    case VALUE:
      if (value == null) {
        unsetValue();
      } else {
        setValue((ByteBuffer)value);
      }
      break;

    }
  }

  public Object getFieldValue(_Fields field) {
    switch (field) {
    case KEY:
      return getKey();

    case VERSION:
      return Long.valueOf(getVersion());

    case CLASSIC_END_VERSION:
      return Long.valueOf(getClassicEndVersion());

    case VALUE:
      return getValue();

    }
    throw new IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new IllegalArgumentException();
    }

    switch (field) {
    case KEY:
      return isSetKey();
    case VERSION:
      return isSetVersion();
    case CLASSIC_END_VERSION:
      return isSetClassicEndVersion();
    case VALUE:
      return isSetValue();
    }
    throw new IllegalStateException();
  }

  @Override
  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof ReadValue)
      return this.equals((ReadValue)that);
    return false;
  }

  public boolean equals(ReadValue that) {
    if (that == null)
      return false;

    boolean this_present_key = true && this.isSetKey();
    boolean that_present_key = true && that.isSetKey();
    if (this_present_key || that_present_key) {
      if (!(this_present_key && that_present_key))
        return false;
      if (!this.key.equals(that.key))
        return false;
    }

    boolean this_present_version = true;
    boolean that_present_version = true;
    if (this_present_version || that_present_version) {
      if (!(this_present_version && that_present_version))
        return false;
      if (this.version != that.version)
        return false;
    }

    boolean this_present_classicEndVersion = true;
    boolean that_present_classicEndVersion = true;
    if (this_present_classicEndVersion || that_present_classicEndVersion) {
      if (!(this_present_classicEndVersion && that_present_classicEndVersion))
        return false;
      if (this.classicEndVersion != that.classicEndVersion)
        return false;
    }

    boolean this_present_value = true && this.isSetValue();
    boolean that_present_value = true && that.isSetValue();
    if (this_present_value || that_present_value) {
      if (!(this_present_value && that_present_value))
        return false;
      if (!this.value.equals(that.value))
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    return 0;
  }

  @Override
  public int compareTo(ReadValue other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;

    lastComparison = Boolean.valueOf(isSetKey()).compareTo(other.isSetKey());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetKey()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.key, other.key);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetVersion()).compareTo(other.isSetVersion());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetVersion()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.version, other.version);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetClassicEndVersion()).compareTo(other.isSetClassicEndVersion());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetClassicEndVersion()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.classicEndVersion, other.classicEndVersion);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetValue()).compareTo(other.isSetValue());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetValue()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.value, other.value);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    return 0;
  }

  public _Fields fieldForId(int fieldId) {
    return _Fields.findByThriftId(fieldId);
  }

  public void read(org.apache.thrift.protocol.TProtocol iprot) throws org.apache.thrift.TException {
    schemes.get(iprot.getScheme()).getScheme().read(iprot, this);
  }

  public void write(org.apache.thrift.protocol.TProtocol oprot) throws org.apache.thrift.TException {
    schemes.get(oprot.getScheme()).getScheme().write(oprot, this);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("ReadValue(");
    boolean first = true;

    sb.append("key:");
    if (this.key == null) {
      sb.append("null");
    } else {
      sb.append(this.key);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("version:");
    sb.append(this.version);
    first = false;
    if (!first) sb.append(", ");
    sb.append("classicEndVersion:");
    sb.append(this.classicEndVersion);
    first = false;
    if (!first) sb.append(", ");
    sb.append("value:");
    if (this.value == null) {
      sb.append("null");
    } else {
      org.apache.thrift.TBaseHelper.toString(this.value, sb);
    }
    first = false;
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws org.apache.thrift.TException {
    // check for required fields
    // check for sub-struct validity
  }

  private void writeObject(java.io.ObjectOutputStream out) throws java.io.IOException {
    try {
      write(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(out)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private void readObject(java.io.ObjectInputStream in) throws java.io.IOException, ClassNotFoundException {
    try {
      // it doesn't seem like you should have to do this, but java serialization is wacky, and doesn't call the default constructor.
      __isset_bitfield = 0;
      read(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(in)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private static class ReadValueStandardSchemeFactory implements SchemeFactory {
    public ReadValueStandardScheme getScheme() {
      return new ReadValueStandardScheme();
    }
  }

  private static class ReadValueStandardScheme extends StandardScheme<ReadValue> {

    public void read(org.apache.thrift.protocol.TProtocol iprot, ReadValue struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TField schemeField;
      iprot.readStructBegin();
      while (true)
      {
        schemeField = iprot.readFieldBegin();
        if (schemeField.type == org.apache.thrift.protocol.TType.STOP) { 
          break;
        }
        switch (schemeField.id) {
          case 1: // KEY
            if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
              struct.key = iprot.readString();
              struct.setKeyIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 2: // VERSION
            if (schemeField.type == org.apache.thrift.protocol.TType.I64) {
              struct.version = iprot.readI64();
              struct.setVersionIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 3: // CLASSIC_END_VERSION
            if (schemeField.type == org.apache.thrift.protocol.TType.I64) {
              struct.classicEndVersion = iprot.readI64();
              struct.setClassicEndVersionIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 4: // VALUE
            if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
              struct.value = iprot.readBinary();
              struct.setValueIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          default:
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
        }
        iprot.readFieldEnd();
      }
      iprot.readStructEnd();

      // check for required fields of primitive type, which can't be checked in the validate method
      struct.validate();
    }

    public void write(org.apache.thrift.protocol.TProtocol oprot, ReadValue struct) throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (struct.key != null) {
        oprot.writeFieldBegin(KEY_FIELD_DESC);
        oprot.writeString(struct.key);
        oprot.writeFieldEnd();
      }
      oprot.writeFieldBegin(VERSION_FIELD_DESC);
      oprot.writeI64(struct.version);
      oprot.writeFieldEnd();
      oprot.writeFieldBegin(CLASSIC_END_VERSION_FIELD_DESC);
      oprot.writeI64(struct.classicEndVersion);
      oprot.writeFieldEnd();
      if (struct.value != null) {
        oprot.writeFieldBegin(VALUE_FIELD_DESC);
        oprot.writeBinary(struct.value);
        oprot.writeFieldEnd();
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class ReadValueTupleSchemeFactory implements SchemeFactory {
    public ReadValueTupleScheme getScheme() {
      return new ReadValueTupleScheme();
    }
  }

  private static class ReadValueTupleScheme extends TupleScheme<ReadValue> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, ReadValue struct) throws org.apache.thrift.TException {
      TTupleProtocol oprot = (TTupleProtocol) prot;
      BitSet optionals = new BitSet();
      if (struct.isSetKey()) {
        optionals.set(0);
      }
      if (struct.isSetVersion()) {
        optionals.set(1);
      }
      if (struct.isSetClassicEndVersion()) {
        optionals.set(2);
      }
      if (struct.isSetValue()) {
        optionals.set(3);
      }
      oprot.writeBitSet(optionals, 4);
      if (struct.isSetKey()) {
        oprot.writeString(struct.key);
      }
      if (struct.isSetVersion()) {
        oprot.writeI64(struct.version);
      }
      if (struct.isSetClassicEndVersion()) {
        oprot.writeI64(struct.classicEndVersion);
      }
      if (struct.isSetValue()) {
        oprot.writeBinary(struct.value);
      }
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, ReadValue struct) throws org.apache.thrift.TException {
      TTupleProtocol iprot = (TTupleProtocol) prot;
      BitSet incoming = iprot.readBitSet(4);
      if (incoming.get(0)) {
        struct.key = iprot.readString();
        struct.setKeyIsSet(true);
      }
      if (incoming.get(1)) {
        struct.version = iprot.readI64();
        struct.setVersionIsSet(true);
      }
      if (incoming.get(2)) {
        struct.classicEndVersion = iprot.readI64();
        struct.setClassicEndVersionIsSet(true);
      }
      if (incoming.get(3)) {
        struct.value = iprot.readBinary();
        struct.setValueIsSet(true);
      }
    }
  }

}

