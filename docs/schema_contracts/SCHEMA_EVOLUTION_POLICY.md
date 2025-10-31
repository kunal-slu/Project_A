# Schema Evolution Policy

This document defines the policy for schema evolution in our data platform, ensuring backward compatibility and safe data processing.

## Principles

1. **Backward Compatibility**: New schemas must be backward compatible with existing data
2. **Forward Compatibility**: Data must be readable by future schema versions (when possible)
3. **Breaking Changes**: Require explicit migration plan and approval
4. **Versioning**: All schemas must be versioned

## Schema Types

### Avro Schemas (Streaming)
- **Location**: `src/pyspark_interview_project/contracts/avro/`
- **Format**: `.avsc` (JSON)
- **Usage**: Kafka/MSK streaming events

### JSON Schemas (Batch)
- **Location**: `config/schema_definitions/`, `src/pyspark_interview_project/contracts/`
- **Format**: `.schema.json`
- **Usage**: Batch data validation at Bronze ingestion

## Evolution Rules

### ✅ Allowed Changes (Backward Compatible)

1. **Adding Fields**
   - New fields must have `default` value or be nullable
   - Example: `"type": ["null", "string"], "default": null`

2. **Making Fields Optional**
   - Change from required to optional (with default)
   - Example: `"name": {"type": "string"}` → `"name": {"type": ["null", "string"], "default": null}`

3. **Widening Types**
   - `int` → `long`
   - `float` → `double`
   - `string` → union with null

4. **Adding Enum Values**
   - New enum values can be added (consumers ignore unknown values)

### ❌ Breaking Changes (Require Migration)

1. **Removing Fields**
   - Fields cannot be removed without deprecation period
   - Must deprecate first, then remove in next major version

2. **Changing Field Types**
   - Narrowing types (e.g., `long` → `int`)
   - Changing types (e.g., `string` → `int`)

3. **Removing Enum Values**
   - Existing data may use removed enum values

4. **Making Required Fields Optional**
   - Can cause nullability issues in downstream processing

5. **Renaming Fields**
   - Breaks existing queries and transforms

## Versioning Strategy

### Semantic Versioning
- **MAJOR**: Breaking changes (requires migration)
- **MINOR**: Backward-compatible additions
- **PATCH**: Documentation/formatting updates

### Example
```
orders_event.avsc v1.0.0 → v1.1.0  (added optional field)
orders_event.avsc v1.1.0 → v2.0.0  (removed deprecated field - breaking)
```

## Migration Process

### Step 1: Plan Migration
1. Document breaking changes
2. Create migration script
3. Estimate downtime/processing impact
4. Get approval from data engineering lead

### Step 2: Implement Migration
1. Deploy new schema version alongside old
2. Run migration job to transform existing data
3. Update all consumers to new schema
4. Monitor for issues

### Step 3: Deprecate Old Schema
1. Mark old schema as deprecated
2. Wait for deprecation period (typically 30 days)
3. Remove old schema version

## Schema Registry Integration

### For Avro Schemas
- Use AWS Glue Schema Registry or Confluent Schema Registry
- Enable compatibility checks: `BACKWARD`, `FORWARD`, `FULL`
- Block incompatible schema updates in CI/CD

### For JSON Schemas
- Validate against `config/config.schema.json` in CI
- Use `jsonschema` Python library for validation

## Examples

### ✅ Safe Addition
```json
{
  "name": "new_field",
  "type": ["null", "string"],
  "default": null,
  "doc": "New optional field"
}
```

### ❌ Breaking Change
```json
{
  "name": "existing_field",
  "type": "int"  // Was "string" - BREAKING!
}
```

## Documentation Requirements

All schema changes must include:
1. **CHANGELOG.md** entry
2. **Migration guide** (if breaking)
3. **Schema version** bump
4. **Test coverage** for new fields

---

**Last Updated**: 2024-01-15  
**Policy Version**: 1.0.0  
**Maintained By**: Data Engineering Team

