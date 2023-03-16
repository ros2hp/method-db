package key

import ()

/*
	type Key struct {
		Name  string // pkey []byte. sortk string
		Value interface{}
	}

// populated in MergeMutation2

	type MergeKey struct {
		Name   string
		Value  interface{}
		DBtype string // db type from TableKey (not type of Value). Used to validate Value.
	}
*/


type Key struct {
	Name  string // pkey []byte. sortk string
	Value interface{}
}
// populated in MergeMutation

	type MergeKey struct {
		Name   string
		Value  interface{}
		DBtype string // db type from TableKey (not type of Value). Used to validate Value.
	}
	
type TableKey struct {
	Name   string // pkey []byte. sortk string
	DBtype string // "S","N","B"
}

type AttributeT map[string]string

type TabMeta struct {
	keys  []TableKey
	attrs AttributeT
}

func NewTabMeta() *TabMeta {
	return &TabMeta{attrs: make(AttributeT)}
}

func (t *TabMeta) GetKeys() []TableKey {
	return t.keys
}

func  (t *TabMeta)GetAttrs() AttributeT {
	return t.attrs
}

func (t *TabMeta) SetKeys(k []TableKey)  {
	t.keys = k
}

func (t *TabMeta) AddKey(col string, ty string) {
	t.keys = append(t.keys, TableKey{Name: col, DBtype: ty})
}

func (t *TabMeta) SetAttrs(k AttributeT)  {
	t.attrs = k
}

func (t *TabMeta) AddAttribute(col string, ty string) {
	t.attrs[col] = ty
}
