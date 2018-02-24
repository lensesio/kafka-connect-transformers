# kafka-connect-transformers

## Transformations

- LogicalFieldToString - Format logical fields value
- MapKeyToString - Replace map key to string value
- NestingFields - Create nesting fields from value to new field

#### LogicalFieldToString
com.datamountaineer.streamreactor.connect.transforms.LogicalFieldToString

Use the concrete transformation type designed for the record key (com.datamountaineer.streamreactor.connect.transforms.LogicalFieldToString$Key) or value (com.datamountaineer.streamreactor.connect.transforms.LogicalFieldToString$Value).

| NAME | DESCRIPTION | TYPE | DEFAULT | VALID VALUES | IMPORTANCE |
| ------ | ------ | ------ | ------ | ------ | ------ |
| fields | Field names to be formatted. |list||non-empty list| high|
| format | Format to apply. |string||valid DecimalFormat, DateFormat format| high|

#### MapKeyToString
com.datamountaineer.streamreactor.connect.transforms.MapKeyToString

Use the concrete transformation type designed for the record key (com.datamountaineer.streamreactor.connect.transforms.MapKeyToString$Key) or value (com.datamountaineer.streamreactor.connect.transforms.MapKeyToString$Value).

| NAME | DESCRIPTION | TYPE | DEFAULT | VALID VALUES | IMPORTANCE |
| ------ | ------ | ------ | ------ | ------ | ------ |
| fields | Field names to be cast. |list||non-empty list| high|

#### NestingFields
com.datamountaineer.streamreactor.connect.transforms.NestingFields

Use the concrete transformation type designed for the record key (com.datamountaineer.streamreactor.connect.transforms.NestingFields$Key) or value (com.datamountaineer.streamreactor.connect.transforms.NestingFields$Value).

| NAME | DESCRIPTION | TYPE | DEFAULT | VALID VALUES | IMPORTANCE |
| ------ | ------ | ------ | ------ | ------ | ------ |
| fields | Field names to add in the nested field. |list||non-empty list| high|
| nested.name | Nested field name. |string||non-empty string| high|
