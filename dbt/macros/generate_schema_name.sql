-- =============================================================================
-- Macro: generate_schema_name
-- Descrição: Sobrescreve o comportamento padrão do dbt para que o schema
--            definido no dbt_project.yml seja usado DIRETAMENTE, sem
--            concatenar com o dataset padrão do profiles.yml.
--
-- Comportamento padrão do dbt:
--   schema = <default_schema>_<custom_schema> → silver_manufacturing_gold_finance
--
-- Com esta macro:
--   schema = <custom_schema> → gold_finance (quando definido)
--   schema = <default_schema> → silver_manufacturing (quando não definido)
-- =============================================================================

{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ default_schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
