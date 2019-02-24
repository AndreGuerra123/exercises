import csv, json, tempfile, statistics

HEADER = ['experiment_name', 'sample_id','fauxness','category_guess']
CATEGORY = ['real','fake','ambiguous']
FORMATS = ['csv','json','dict']

class FauxDialect(csv.Dialect): # This could be further improved to improve the robustness of Faux files.
    delimiter = ',' # Required
    lineterminator = '\t\n' #Implementation specific.
    quotechar = '"' #Implementation specific.
    quoting = csv.QUOTE_ALL #Implementation specific.

DIALECT = FauxDialect()

class Fauxlizer:
    def __init__(self,filename:str):        
        self.fn = filename
        self._file_exist()
        self._is_fauxer_file()

    def _file_exist(self):
        try:
            open(self.fn,'r')   
        except FileNotFoundError:
            raise FileNotFoundError('{} file does not exist.'.format(self.fn))

    def _is_fauxer_file(self): # Does not check for MIME, checks for readibility and explicity dialect attributes.#See dialect this values should match.
        try:
            with open(self.fn, 'r') as file:
                dialect = csv.Sniffer().sniff(file.read(1024))
                if dialect.delimiter != ',':
                    raise csv.Error('Cell delimiter is not "," .')
                file.seek(0)
        except csv.Error as e:
            raise FileExistsError('{} file is not a valid Fauxlizer (.faux) file. {}'.format(self.fn,str(e)))

    def _generator(self): # Data is acquired throw a generator to relief memory issues and to enumerate the rows read.
        with open(self.fn, 'r') as file:
            data = csv.reader(file)
            for idx,row in enumerate(data):
                yield (idx,row)

    def _format_row(self,obs:list,form='json'): #takes the validated, ordered observation and the format (optional) argument
        if form not in FORMATS:
            raise ValueError('%s is a invalid format option. Valid options include %s'.format(format,str(FORMATS)))
 
        if form == 'csv':
            f = tempfile.TemporaryFile() # Creates a tempfile to return to user
            f.close()
            fs = open(f.name, 'w')
            writer = csv.writer(fs,dialect=DIALECT) #writes accordingly to dialect of faux files
            writer.writerow(HEADER)
            writer.writerow(obs)
            return f.name # returns the newly created file filename, The decision of returning the path is based on the common way of serving static files to users in a API.
        else:
            final_dict = dict(zip(HEADER, obs))
            if form == 'json':
                return json.dumps(final_dict) # dumps dictionary to a json string
            else:
                return final_dict # returns the in-memory dictionary

    def _is_na(self,x:str):
        if x.strip().upper() in ['NA','NAN','','NULL','NONE']:
            return True
        return False

    def _is_header(self,x:str):
        for xx in x:
            if xx in HEADER:
                return True
        return False

    def _is_class(self,x:str):
        if x in CATEGORY:
            return True
        else:
            return False

    def _get_class_idx(self,i:int,r:list):
        class_map = map(self._is_class,r)
        idx = [i for i, x in enumerate(class_map) if x]
        if len(idx) > 1:
            raise ValueError('{}th row ({}) contains ambiguous classification value.'.format(str(i),str(r)))
        elif len(idx) == 0:
            raise ValueError('{}th row ({}) contains missing classification value.'.format(str(i),str(r)))
        else:
            return int(idx[0])

    def _is_fauxer(self,x:str):
        try:
            if float(x) >= 0.0 or float(x) <= 1.0 :
                return True
            return False
        except:
            return False

    def _get_fauxer_idx(self,i:int,r:list,id:int):
        fauxer_map = map(self._is_fauxer,r)
        idx = [i for i, x in enumerate(fauxer_map) if x]
        idx.remove(id)
        if len(idx) > 1:
            raise ValueError('{}th row ({}) contains ambiguous fauxer value.'.format(str(i),str(r)))
        elif len(idx) == 0:
            raise ValueError('{}th row ({}) contains missing fauxer value.'.format(str(i),str(r)))
        else:
            return idx[0]

    def _is_sample_id(self,x:str):
        try:
            if int(x) > 0:
                return True
            return False
        except:
            return False

    def _get_sample_id_idx(self,i:int,r:list):
        sample_map = map(self._is_sample_id,r)
        idx = [i for i, x in enumerate(sample_map) if x]
        if len(idx) > 1:
            raise ValueError('{}th row ({}) contains ambiguous sample ID value.'.format(str(i),str(r)))
        elif len(idx) == 0:
            raise ValueError('{}th row ({}) contains missing sample ID value.'.format(str(i),str(r)))
        else:
            return idx[0]

    def _get_experiment_name_idx(self,i:int,r:list,e:list):
        idx = [x for x in range(0,4) if x not in set(e)]
        if len(idx) > 1:
            raise ValueError('{}th row ({}) contains ambiguous experiment name value.'.format(str(i),str(r)))
        elif len(idx) == 0 or self._is_na(r[idx[0]]):
            raise ValueError('{}th row ({}) contains missing experiment name value.'.format(str(i),str(r)))
        else:
            return idx[0]

    
    def _validate_row(self,i:int,r:list): #validates and orders the row, the main advantage of this approach is that it does not need the header. It is based on the validation rules and "good" assumptions.
        if(len(r)!=4):
            raise ValueError('{}th row ({}) seems to be corrupted (size != 4).'.format(str(i),str(r)))
        elif self._is_header(r):
            raise ValueError('{}th row ({}) seems to be the header.'.format(str(i),str(r)))
        else:
            c_idx = self._get_class_idx(i,r)
            sid_idx = self._get_sample_id_idx(i,r)
            f_idx = self._get_fauxer_idx(i,r,sid_idx)
            exp_idx = self._get_experiment_name_idx(i,r,[c_idx,f_idx,sid_idx])
            return [r[exp_idx],int(r[sid_idx]),float(r[f_idx]),r[c_idx]]

    def get_row(self,idx:int,form:str='json'): # API calls this to read the file row (idx th) with the format required
        for i, r in self._generator():
            if i == idx:
                return self._format_row(self._validate_row(i,r),form)
        raise ValueError('{}th row does not exist in the {} file.'.format(str(idx),self.fn))

    def _get_faux_data(self,valid:dict):
        to_return = []
        for key, value in valid.items():
            to_return.append(value[2])
        return to_return

    def _get_stats(self,fd:list):
        if len(fd) > 1:
            return {'mean':statistics.mean(fd),
                'median':statistics.median(fd),
                'std':statistics.pstdev(fd),
                'var':statistics.variance(fd)}
        else:
            return None

    def get_summary(self):
        headers = {}
        valid_samples= {}
        invalid_samples = {}
        for i,r in self._generator():
            if self._is_header(r):
                headers[str(i)] = str(r)
            else:
                try:
                    l = self._validate_row(i,r)
                    valid_samples[str(i)] = l
                except ValueError as e:
                    invalid_samples[str(i)]=str(e)
        faux_stats = self._get_stats(self._get_faux_data(valid_samples))
        return json.dumps({'headers':headers,'valid_samples':valid_samples,'invalid_samples':invalid_samples,'faux_stats':faux_stats})



