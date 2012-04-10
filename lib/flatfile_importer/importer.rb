module FlatfileImporter
  class Importer
    attr_accessor :spreadsheet
    attr_accessor :column_indices
    
    def initialize(filepath)
      read_excel(filepath)
      detect_columns
    end
  
    def import!
      process_lines
    end
  
    def read_excel(filepath)
      begin
        require 'iconv'
        @spreadsheet = Excel.new(filepath)
        @spreadsheet.default_sheet = @spreadsheet.sheets.first
      rescue TypeError => err
        raise err.message
      end
    end
    
    # Return array of all allowable alternative column labels for given column
    # Matching is case insensitive (case doesn't matter)
    def synonym_column_labels(col_name)
      [col_name.gsub('_', ' ')]
    end
    
    def detect_columns
      attrs = [primary_key_attribute] +
              primary_mass_assignable_attributes +
              primary_complex_attributes
      joins.each do |join|
        attrs += keys_for(join)
        attrs += attributes_for(join)
      end
      @column_indices = {}
      actual_column_labels = @spreadsheet.row(1).map(&:downcase)
      attrs.each do |at|
        ats = at.to_s.downcase
        index = actual_column_labels.index { |item| [ats, synonym_column_labels(ats).map(&:downcase)].include?(item) }
        if index
          logger.info("#{at} is column #{index+1}")
          @column_indices[at] = index+1
        else
          raise "Couldn't find a header cell labelled \"#{at}\". Nothing imported."
        end
      end
    end
  
    def primary_mass_assignable_attributes
      raise "implement me"
    end
  
    def primary_complex_attributes
      raise "implement me"
    end
  
    def joins
      raise "implement me"
    end
  
    def keys_for(join)
      raise "implement me"
    end
  
    def attributes_for(join)
      raise "implement me"
    end
  
    # How we key a primary record
    def primary_key_attribute
      raise "implement me"
    end
  
    def assign_complex_attribute(record, attr_name, value)
    end
  
    def primary_record_endpoint
      raise "implement me"
    end
  
    def cell_value(line, col_label)
      @spreadsheet.cell(line, @column_indices[col_label]).to_s
    end
  
    def process_lines
      @primary_records = {}
      @to_save = Set.new
      import_time = DateTime.now
        
      (2..@spreadsheet.last_row).each do |line|
        ms = time_block_ms {
          process_line(line)
        }
        logger.info("Processing line #{line} took #{ms} ms")
        #@organisation.inc(:import_progress, 1)
      end
    
      results = {}
      @to_save.each do |record|
        clazz = record.class
        results[clazz] ||= {}
        results[clazz][:updated] ||= []
        results[clazz][:created] ||= []
        results[clazz][:invalid] ||= []
        existing = !record.new_record?
        saved = false
        ms = time_block_ms {
          record.last_imported_at = import_time if record.respond_to?(:last_imported_at)
          saved = record.save
        }
        logger.info("Saving #{'existing ' if existing}#{record.class.name} #{record.id} took #{ms} ms")
        #@organisation.inc(:import_progress, 1)
        if saved
          if existing
            results[clazz][:updated] << record
          else
            results[clazz][:created] << record
          end
        else
          results[clazz][:invalid] << record
        end
      end
    
      results
    end
    
    def logger
      Rails.logger
    end
  
  private
   
    def process_line(line)
      primary_key = cell_value(line, primary_key_attribute)
      logger.info("processing line with primary key #{primary_key}")
    
      unless primary_key.blank?
        primary_record = @primary_records[primary_key]
        if !primary_record
          logger.info("found primary record #{primary_record}")
          primary_record = primary_record_endpoint.respond_to?(:where) ?
            primary_record_endpoint.where(primary_key_attribute => primary_key).first :
            primary_record_endpoint.find(:first, :conditions => {primary_key_attribute => primary_key})
          if primary_record
            logger.info("found existing #{primary_key}")
          else
            logger.info("creating #{primary_key}")
            primary_record = primary_record_endpoint.build(
              primary_key_attribute => primary_key
            )
          end
        
          # Assign simple attributes
          primary_mass_assignable_attributes.map do |attr_name|
            primary_record.send("#{attr_name}=", cell_value(line, attr_name))
          end
        
          # Handle attributes/relations with custom import behaviour
          primary_complex_attributes.each do |attr_name|
            assign_complex_attribute(primary_record, attr_name, cell_value(line, attr_name))
          end
        else
          logger.info("already seen #{primary_key}")
        end
      
        # Now deal with sub records
        joins.each do |join|
          keys = Hash[*keys_for(join).map {|k| [k, cell_value(line, k)]}.flatten]
          # Might have to load whole relation and do manual detect to get autosaving working here
          secondary = primary_record.send(join).detect do |sec|
            keys.all? do |k,v|
              sec.send(k) == v
            end
          end
          if secondary
            logger.info("found secondary with keys #{keys.values.join(', ')}")
          else
            logger.info("creating new secondary for keys #{keys.values.join(', ')}")
            secondary = primary_record.send(join).build(keys)
          end
        
          # Assign simple attributes
          attributes_for(join).map do |attr_name|
            secondary.send("#{attr_name}=", cell_value(line, attr_name))
          end
        
          @to_save << secondary unless primary_record.new_record?
        end
      
        @primary_records[primary_key] = primary_record
        @to_save << primary_record
      
      end
    end
  
    def time_block_ms
      beginning_time = Time.now
      yield
      end_time = Time.now
      duration = (end_time - beginning_time)*1000
    end
    
  end
end