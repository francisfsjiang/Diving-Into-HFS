#! ruby -w
# -*- coding: utf-8 -*-

require 'erb'
require 'yaml'

class JavaClass
	# 阈值自己调
	CUT_OFF_WIDTH = 120

	DOT_TEMPLATE = ERB.new  <<-ERB
	<%= name.gsub('$', '_') %>[layer=<%= JavaClass.id %>,label=<
<TABLE BORDER='1' CELLBORDER='0' CELLSPACING='0'>

<TR><TD>
	<% if abstract %>
		<I>
	<% end %>
	<% if static %>
		<%= ERB::Util.h("<<static>>") %><BR/>
	<% end %>
	<%= ERB::Util.html_escape name %><%= final && ERB::Util.html_escape(" {leaf}") %>
	<% if abstract %>
		</I>
	<% end %>
</TD></TR>

<HR/>

<% if fields.empty? %>
<TR><TD><BR/></TD></TR>
<% end %>

<% fields.each do |field| %>
<TR><TD ALIGN='left'>
	<% if field.static %>
		<U>
	<% end %>
	<%= ERB::Util.html_escape field.to_s[0...CUT_OFF_WIDTH] %>
	<% if field.static %>
		</U>
	<% end %>
</TD></TR>
<% end %>

<HR/>

<% if methods.empty? %>
<TR><TD><BR/></TD></TR>
<% end %>
<% methods.each do |method| %>
<TR><TD ALIGN='left'>
	<% if method.abstract %>
		<I>
	<% end %>
	<% if method.static %>
		<U>
	<% end %>
		<%= ERB::Util.html_escape method.to_s[0...CUT_OFF_WIDTH] %>
	<% if method.static %>
		</U>
	<% end %>
	<% if method.abstract %>
		</I>
	<% end %>
</TD></TR>
<% end %>

</TABLE>
>]
<% inners.each do |inner| %>
<%= inner.to_dot %>
<% end %>
	ERB

	def self.format_access(access)
		case access
		when /public/i    then '+'
		when /protected/i then '#'
		when /private/i   then '-'
		when /package/i   then '~'
		else                   access
		end
	end

	def self.id
		@id ||= 0
		@id += 1
	end

	class Field
		attr_reader :name, :type, :access, :final, :static

		def initialize(data)
			@name, @type = data['name'], data['type']
			@access = JavaClass.format_access data['access']
			@static, @final = data['final'], data['static']
		end

		def to_s
			"%{access} %{name}: %{type}%{final}" % {
				access: access,
				name: name,
				type: type,
				final: final && " {readonly}"
			}
		end
	end

	class Method
		attr_reader :name, :type, :args, :access, :final, :static, :abstract

		def initialize(data)
			@name, @type, @args = data['name'], data['type'], data['args'] || []
			@access = JavaClass.format_access data['access']
			@static, @final, @abstract = data['final'], data['static'], data['abstract']
		end

		def to_s
			"%{access} %{name}(%{args}): %{type}%{final}" % {
				access: access,
				name: name,
				type: type,
				args: args.map{ |arg|
					"%{name}:%{type}" % { name: arg['name'], type: arg['type'] }
				}.join(', '),
				final: final && " {leaf}"
			}
		end
	end

	attr_reader :name, :fields, :methods, :inners, :final, :static, :abstract

	def initialize(data)
		@name = data['name']
		@fields = (data['fields'] || []).map{|field| Field.new(field) }
		@methods = (data['methods'] || []).map{|method| Method.new(method) }
		@inners = (data['inners'] || []).map do |inner|
			inner['name'] = name + '$' + inner['name']
			JavaClass.new(inner)
		end
		@static, @final, @abstract = data['static'], data['final'], data['abstract']
	end

	def to_dot
		DOT_TEMPLATE.result(binding).strip
	end
end

DOT_TEMPLATE = ERB.new  <<-ERB
graph Class {
	graph [rankdir="LR"]
	node [fontname="DejaVu Sans Mono"]
	node [shape=plaintext]
<%= contents %>
}
ERB

SRC_PATTERN = File.join('gen', 'yaml', '*.yaml')
DOT_PATH = File.join('gen', 'dot')
OUT_PATH = File.join('image', 'gen')

Dir.mkdir DOT_PATH unless Dir.exist? DOT_PATH
Dir.mkdir OUT_PATH unless Dir.exist? OUT_PATH

Dir.glob(SRC_PATTERN) do |filename|
	YAML.load_file(filename).each do |klass|
	# YAML.load(DATA).each do |klass|
		dot_file = File.join(DOT_PATH, "#{klass['name']}.dot")
		out_file = File.join(OUT_PATH, "#{klass['name']}.pdf")
		java_class = JavaClass.new(klass)

		contents = java_class.to_dot

		File.write(dot_file, DOT_TEMPLATE.result(binding).strip)
		p system('dot', "-Tpdf", "-o#{out_file}", dot_file)
	end
end

__END__
---
- name: NormalClass
  fields:
  - name: normalField
    type: Normal
    access: public
  - name: staticField
    type: Static
    access: package
    static: true
  - name: finalField
    type: Final
    access: protected
    final: true
  methods:
  - name: normalMethod
    type: Normal
    args:
    - name: argument
      type: String
    access: public
  - name: staticMethod
    type: Static
    args:
    - type: int
    - type: long
    access: package
    static: true
  - name: finalMethod
    type: Static
    final: true
    access: protected
  - name: abstractMethod
    type: Abstract
    access: private
    abstract: true
  inners:
