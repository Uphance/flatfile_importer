module FlatfileImporter
  class Importer
    attr_accessor :spreadsheet
    attr_accessor :column_indices
    attr_reader :import_time
    attr_accessor :set_imported_at
    
    def initialize(filepath)
      self.set_imported_at = true
      case File.extname(filepath)
      when '.xls'
        read_excel(filepath)
      when '.csv'
        read_csv(filepath)
      end
    end
  
    def import!
      detect_columns
      process_lines
    end
    
    def read_csv(filepath)
      begin
        require 'iconv'
        @spreadsheet = Csv.new(filepath)
      rescue TypeError => err
        raise err.message
      end
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
    def acceptable_column_labels_for_attribute(attr_name, join)
      [attr_name, attr_name.gsub('_', ' ')]
    end
    
    def detect_columns
      attrs = primary_record_key_cols +
              primary_mass_assignable_attributes +
              primary_complex_attributes
      detect_columns_for(attrs, nil)
      joins.each do |join|
        detect_columns_for(keys_for(join), join)
        detect_columns_for(attributes_for(join), join)
        detect_columns_for(secondary_complex_attributes_for(join), join)
      end
    end
    
    def detect_columns_for(attrs, join = nil)
      @column_indices ||= {}
      actual_column_labels = @spreadsheet.row(1).map(&:downcase)
      attrs.map(&:to_s).map(&:downcase).each do |attr|
        column_labels = acceptable_column_labels_for_attribute(attr, join).map(&:downcase)
        index = actual_column_labels.index { |item| column_labels.include?(item) }
        if index
          logger.info("#{attr} is column #{index+1}")
          @column_indices[[join, attr].compact.join('.')] = index+1
        else
          raise "Couldn't find a header cell for \"#{attr}\". Looked for one of #{column_labels.join(', ')}. Nothing imported."
        end
      end
    end
  
    def primary_mass_assignable_attributes
      raise "implement me"
    end
  
    def primary_complex_attributes
      raise "implement me"
    end
    
    def secondary_complex_attributes_for(join)
      []
    end
    
    def joins
      []
    end
  
    def keys_for(join)
      raise "implement me"
    end
  
    def attributes_for(join)
      raise "implement me"
    end
  
    def assign_complex_attribute(record, attr_name, value)
    end
    
    # Set of column values that uniquely identity a primary record (used for maintaining)
    # cache of primary records as we iterate over the spreadsheet
    def primary_record_key_cols
      raise "implement me"
    end
    
    def find_primary_record(line)
      raise "implement me"
    end
    
    def build_primary_record(line)
      raise "implement me"
    end
  
    def cell_value(line, col_label, join = nil)
      col_label = col_label.to_s
      col_label = "#{join}.#{col_label}" if join.present?
      @spreadsheet.cell(line, @column_indices[col_label.downcase]).to_s
    end
  
    def process_lines
      @primary_records = {}
      @to_save = Set.new
      @import_time = Time.now.utc
        
      (2..@spreadsheet.last_row).each do |line|
        ms = time_block_ms {
          process_line(line)
        }
        logger.info("Processing line #{line} took #{ms} ms")
        #@organisation.inc(:import_progress, 1)
      end
      
      about_to_save_records(@to_save)
      
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
          if self.set_imported_at && record.respond_to?(:last_imported_at)
            record.last_imported_at = @import_time
          end
          saved = record.save
        }
        logger.info("Saving #{'existing ' if existing}#{record.class.name} #{record.id} took #{ms} ms")
        #@organisation.inc(:import_progress, 1)
        about_to_save_record(record)
        if saved
          saved_record(record, true)
          if existing
            results[clazz][:updated] << record
          else
            results[clazz][:created] << record
          end
        else
          logger.warn "Invalid import: #{record.errors.full_messages.to_sentence}"
          saved_record(record, false)
          results[clazz][:invalid] << record
        end
      end
      
      finished_saving_records(@to_save)
    
      results
    end
    
    def logger
      Rails.logger
    end
    
    def about_to_save_records(records)
    end
    
    def about_to_save_record(record)
    end
    
    def saved_record(record, success)
    end
    
    def finished_saving_records(records)
    end
  
  private
   
    def process_line(line)
      # Just used for caching primary record
      primary_key = primary_record_key_cols.map {|col| [col, cell_value(line, col)]}
      logger.info("processing line with primary key #{primary_key}")
    
      unless primary_key.all? {|pair| pair.second.blank?}
        primary_record = @primary_records[primary_key]
        if !primary_record
          logger.info("found primary record #{primary_record}")
          primary_record = find_primary_record(line) || build_primary_record(line)
          if !primary_record.new_record?
            logger.info("found existing")
          else
            logger.info("created new record")
          end
        
          # Assign simple attributes
          primary_mass_assignable_attributes.map do |attr_name|
            value = cell_value(line, attr_name)
            logger.info("assigning #{attr_name} = #{value}")
            primary_record.send("#{attr_name}=", value)
          end
        
          # Handle attributes/relations with custom import behaviour
          primary_complex_attributes.each do |attr_name|
            assign_complex_attribute(primary_record, attr_name, line)
          end
        else
          logger.info("already seen #{primary_key}")
        end
      
        # Now deal with sub records
        joins.each do |join|
          keys = Hash[*keys_for(join).map {|k| [k, cell_value(line, "#{join}.#{k}")]}.flatten]
          if keys.values.all?(&:blank?) # reject if all keys blank
            logger.info "Skipping #{join} on line #{line} because all keys are blank"
            next
          end
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
            secondary.send("#{attr_name}=", cell_value(line, "#{join}.#{attr_name}"))
          end
          
          # Handle attributes/relations with custom import behaviour
          secondary_complex_attributes_for(join).each do |attr_name|
            assign_complex_attribute(secondary, attr_name, line)
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