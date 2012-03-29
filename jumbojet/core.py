"""
ideas - machine learning for looking at data. 
Pycon topic, squirrel identification system

etl - system for healthcare data.
"""
import csv
import os
import datetime
from dateutil.parser import parse
from collections import OrderedDict
import threading
import json

import time
NULL_VALUES = ('na', 'n/a', 'none', 'null', '.')
TRUE_VALUES = ('yes', 'y', 'true', 't')
FALSE_VALUES = ('no', 'n', 'false', 'f')

DEFAULT_DATETIME = datetime.datetime(9999, 12, 31, 0, 0, 0)
NULL_DATE = datetime.date(9999, 12, 31)
NULL_TIME = datetime.time(0, 0, 0)
def convert_type(item):
    if item is not None and item.lower() in NULL_VALUES:
        item = ''
    return item

def is_null(item):
    if item.strip() == '' or item is None:
        return True
    return False

def is_bool(item):
    if item.lower() in TRUE_VALUES or item.lower() in FALSE_VALUES:
        return True
    return False

def is_int(item):
    if is_null(item):
        item = '0'
    try:
        int(item.replace( ',', ''))
        if item[0] == '0' and int(item) != 0:
            raise TypeError('Integer is padded with 0s, so treat it as a string instead.')
    except TypeError:
        return False
    except ValueError:
        return False
    return True

def is_float(item):
    if is_null(item):
        item = '0.0'
    try:
        float(item.replace(',',''))
    except ValueError:
        return False
    return True

def is_date(item):
    pass

def is_datetime(item):
    if not item.strip() or len(item) < 5:
        return False
    try:
        parse(item, default=DEFAULT_DATETIME)
    except ValueError:
        return False
    except OverflowError:
        return False
    except TypeError:
        return False
    return True
class Column(object):
    def __init__(self, name):
        self.name = name
        self.nullable=False
        self.col_type = None
        self.length=0
        self.type_index=-1
        self.values = set([])
        self.lock = threading.Lock()

    def to_dict(self):
        d = OrderedDict()
        d['name'] = self.name.lower()
        d['nullable']=self.nullable
        d['type'] = self.col_type
        d['sample_values']=[val for val in self.values]
        if self.length:
            d['length'] = self.length
        return d

    @classmethod
    def from_dict(cls, d):
        c = cls(d['name'])
        c.nullable = d['nullable']
        c.col_type = d['type']
        c.values = set(d['sample_values'])
        c.length = d.get('length', None)
        return c

    def __eq__(self, other):
        return self.name == other.name and self.nullable == other.nullable and self.col_type == other.col_type and self.length == other.length

    def __ne__(self, other):
        return not self.__eq__(other)

    def __str__(self):
        s = """%(name)s
        type: %(type)s
        nullable: %(nullable)s
        sample_values: %(sample_values)s
        """
        return s % self.to_dict()

def do_stuff(filename, override):
    f_list = [is_bool, is_int, is_float, is_datetime]

    start = time.clock()
    #read file in
    fl = open(filename,'rb')
    reader = csv.DictReader(fl)
    fieldnames = reader.fieldnames
    #[columns.setdefault(c,{}) for c in fieldnames]
    

    columns = [Column(name) for name in fieldnames]
    def process_row(row):
        for index, field in enumerate(fieldnames):
            col = columns[index]
            value = convert_type(row[field])
            current_index = col.type_index
            if is_null(value):
                col.nullable = True
            if len(col.values) < 5:
                col.values.add(value)

            #determine type
            r_list = [f(value) for f in f_list]
            #which type was it.
            try:
                true_index = r_list.index(True)
            except ValueError:
                true_index=100

            if true_index > current_index:
                if true_index == 100:
                    f_type = 'str'
                    col.length = col.length if col.length >= len(value) else len(value)
                else:
                    f = f_list[true_index]
                    f_type = f.__name__.split('_')[1]
                col.type_index = true_index
                col.col_type = f_type
                col.values.add(value)
            columns[index] = col
    map(process_row, reader)
    #for row in reader:
    #    process_row(row)

    fl.close()
    end = time.clock()
    print end-start
    new_file = open('%s.json' % filename,'w')
    js = [c.to_dict() for c in columns]
    json.dump(js, new_file)
    new_file.close()
    return columns, js

def parse_overrides(olist):
    #join overrides
    joined = ''.join(olist)
    joined.split(',')
    #dict([i.split(':') for i in l])
    return {}

type_to_field = {
    'str':'    %(name)s = models.Charfield(max_length=%(length)s, null=%(nullable)s)',
    'int': '    %(name)s = models.IntegerField(null=%(nullable)s)',
    'datetime': '    %(name)s = models.DateTimeField(null=%(nullable)s)',
    'bool': '    %(name)s = models.BooleanField(null=%(nullable)s)',
    'float': '    %(name)s = models.FloatField(null=%(nullable)s)',
    'decimal': '    %s(name)s = models.DecimalField()'
}
def build_column(cdict):
    if cdict:
        cstring = type_to_field.get(cdict['type'], None)
        return cstring % cdict
    return ''

def build_model(name, columns):
    model = """class %s(models.Model):\n"""
    model = model % name
    for key, col in columns.items():
        cstring = build_column(col)
        model += cstring + '\n'
    print model


def parse_csv(filename):
    #builds dictionary representation of csv file
    f_list = [is_bool, is_int, is_float, is_datetime]

    start = time.clock()
    #read file in
    fl = open(filename,'rb')
    reader = csv.DictReader(fl)
    fieldnames = reader.fieldnames
    #[columns.setdefault(c,{}) for c in fieldnames]
    

    columns = [Column(name) for name in fieldnames]
    def process_row(row):
        for index, field in enumerate(fieldnames):
            col = columns[index]
            value = convert_type(row[field])
            current_index = col.type_index
            if is_null(value):
                col.nullable = True
            if len(col.values) < 5:
                col.values.add(value)

            #determine type
            r_list = [f(value) for f in f_list]
            #which type was it.
            try:
                true_index = r_list.index(True)
            except ValueError:
                true_index=100

            if true_index > current_index:
                if true_index == 100:
                    f_type = 'str'
                    col.length = col.length if col.length >= len(value) else len(value)
                else:
                    f = f_list[true_index]
                    f_type = f.__name__.split('_')[1]
                col.type_index = true_index
                col.col_type = f_type
                col.values.add(value)
            columns[index] = col
    map(process_row, reader)
    #for row in reader:
    #    process_row(row)

    fl.close()
    end = time.clock()
    js = OrderedDict()
    for c in columns:
        js[c.name.lower()] = c.to_dict()
    return js

def parse_json(filename, class_name):
    fp = open(filename, 'rb')
    js = json.load(fp, object_pairs_hook=OrderedDict)
    build_model(class_name,js)

class DictDiffer(object):
    """
    Calculate the difference between two dictionaries as:
    (1) items added
    (2) items removed
    (3) keys same in both but changed values
    (4) keys same in both and unchanged values
    """
    def __init__(self, current_dict, past_dict):
        self.current_dict, self.past_dict = current_dict, past_dict
        self.set_current, self.set_past = set(current_dict.keys()), set(past_dict.keys())
        self.intersect = self.set_current.intersection(self.set_past)

    def same(self):
        return self.intersect

    def different(self):
        return self.set_current ^ self.set_past
    def added(self):
        return self.set_current - self.intersect 
    def removed(self):
        return self.set_past - self.intersect 
    def changed(self):
        new_set = set([])
        for o in self.intersect:
            c1 = Column.from_dict(self.past_dict[o])
            c2 = Column.from_dict(self.current_dict[o])
            if c1 != c2:
                new_set.add(o)
        return new_set
    def unchanged(self):
        new_set = set([])
        for o in self.intersect:
            if Column.from_dict(self.past_dict[o]) == Column.from_dict(self.current_dict[o]):
                new_set.add(o)
        return new_set

    def __getitem__(self, key):
        #get key from current or past
        return (self.current_dict.get(key,''),self.past_dict.get(key,''))

import difflib
def print_diff(filename1, filename2, d1, d2):
    for line in difflib.unified_diff([build_column(d1)],[build_column(d2)],filename1, filename2): 
        print line

def compare_json(file1, file2):
    #load files
    current = json.load(open(file1, 'rb'),object_pairs_hook=OrderedDict)
    filename1 = os.path.basename(file1)
    past = json.load(open(file2, 'rb'), object_pairs_hook=OrderedDict)
    filename2 = os.path.basename(file2)
    differ = DictDiffer(current, past)
    #print differ.same()
    #print differ.different()
    print "========= CHANGED:"
    #print differ.changed()
    for  key in differ.changed():
        o,p = differ[key]
        print_diff(filename1, filename2, o,p)
        
    #print "========= UNCHANGED:"
    #print differ.unchanged()
    print "========= DIFFERENT"
    for key in differ.different():

        o,p = differ[key]
        print_diff(filename1, filename2, o,p)

def transform(file1, new):
    """transforms json to new version"""
    js = json.load(open(file1, 'rb'))
    new_js = OrderedDict()
    for c in js:
        new_js[c['name']] = c
    json.dump(new_js, open(new,'w'), indent=4)
