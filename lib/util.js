var types = require('cassandra-driver').types,
  Uuid = types.Uuid,
  TimeUuid = types.TimeUuid,
  Integer = types.Integer,
  BigDecimal = types.BigDecimal,
  InetAddress = types.InetAddress,
  Long = require('long');
var _ = require('lodash');

/**
 * Cast value from Cassandra data type to Waterline type
 *
 * @param value
 * @returns {*}
 */
exports.castFromCassandraToWaterline = function(value, waterlineTypeHint) {
  var ret;
  if (value instanceof Uuid) {
    ret = value.toString();
  }
  else if (value instanceof InetAddress) {
    ret = value.toString();
  }
  else if (value instanceof Integer) {
    ret = value.toNumber();
  }
  else if (value instanceof BigDecimal) {
    ret = value.toNumber();
  }
  else if (value instanceof Long) {
    ret = value.toNumber();
  }
  else if(waterlineTypeHint) {
    switch(waterlineTypeHint) {
      case 'json':
        ret = JSON.parse(value);
        break;
      case 'array':
        ret = _.map(value, function(item) {
            try {
                return JSON.parse(item);
            } 
            catch(e) {
                return item;
            }
        });
        break;
      default:
        ret = value;
        break;
    }
  }
  else {
    ret = value;
  }
  return ret;
};


/**
 * Map Waterline data type string to Cassandra data type string.
 *
 * @param waterlineDataType
 * @returns {String} cassandra data type
 * @private
 */
exports.mapWaterlineTypeToCassandra = function(waterlineDataType, waterlineAttribute) {

  switch(waterlineDataType) {

    case 'string':
    case 'text':
    case 'json':
      return 'text';

    case 'email':
      return 'ascii';

    case 'integer':
      return 'bigint';

    case 'float':
      return 'double';

    case 'boolean':
      return 'boolean';

    case 'date':
    case 'datetime':
      return 'timestamp';

    case 'binary':
      return 'blob';

    case 'array':
      if(waterlineAttribute && waterlineAttribute.unique)
        return 'set<text>';
      return 'list<text>';

    default:
      console.error("Unregistered type '%s'. Treating as 'text'.", type);
      return 'text';
  }
};

exports.mapWaterlineTypeValueToCassandra = function(value, waterlineDataType) {
  switch(waterlineDataType) {
    case 'json':
      return JSON.stringify(value);
    case 'array':
      return _(value).map(JSON.stringify).value();
    default:
      return value;
   }
};
