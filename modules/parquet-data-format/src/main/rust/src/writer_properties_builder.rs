/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

use parquet::basic::{Compression, ZstdLevel, GzipLevel, BrotliLevel};
use parquet::file::properties::WriterProperties;

use crate::native_settings::NativeSettings;

/// Builder for converting NativeSettings into Parquet WriterProperties.
pub struct WriterPropertiesBuilder;

impl WriterPropertiesBuilder {
    /// Builds WriterProperties from a NativeSettings.
    pub fn build(config: &NativeSettings) -> WriterProperties {
        let mut builder = WriterProperties::builder();
        builder = Self::apply_compression_settings(builder, config);
        builder = Self::apply_page_settings(builder, config);
        builder = Self::apply_dictionary_settings(builder, config);
        builder = Self::apply_field_configs(builder, config);
        builder.build()
    }

    fn apply_compression_settings(
        mut builder: parquet::file::properties::WriterPropertiesBuilder,
        config: &NativeSettings,
    ) -> parquet::file::properties::WriterPropertiesBuilder {
        let compression = Self::parse_compression_type(
            config.get_compression_type(),
            config.get_compression_level(),
        );
        builder = builder.set_compression(compression);
        builder
    }

    fn apply_page_settings(
        mut builder: parquet::file::properties::WriterPropertiesBuilder,
        config: &NativeSettings,
    ) -> parquet::file::properties::WriterPropertiesBuilder {
        builder = builder.set_data_page_size_limit(config.get_page_size_bytes());
        builder
    }

    fn apply_dictionary_settings(
        mut builder: parquet::file::properties::WriterPropertiesBuilder,
        config: &NativeSettings,
    ) -> parquet::file::properties::WriterPropertiesBuilder {
        builder = builder.set_dictionary_page_size_limit(config.get_dict_size_bytes());
        builder
    }

    fn apply_field_configs(
        mut builder: parquet::file::properties::WriterPropertiesBuilder,
        config: &NativeSettings,
    ) -> parquet::file::properties::WriterPropertiesBuilder {
        if let Some(field_configs) = &config.field_configs {
            for (field_name, field_config) in field_configs {
                if let Some(compression_type) = &field_config.compression_type {
                    let compression = Self::parse_compression_type(
                        compression_type,
                        field_config.compression_level.unwrap_or(3),
                    );
                    builder = builder.set_column_compression(field_name.clone().into(), compression);
                }
            }
        }
        builder
    }

    fn parse_compression_type(compression_type: &str, level: i32) -> Compression {
        match compression_type.to_uppercase().as_str() {
            "ZSTD" => Compression::ZSTD(
                ZstdLevel::try_new(level).unwrap_or(ZstdLevel::default()),
            ),
            "SNAPPY" => Compression::SNAPPY,
            "GZIP" => Compression::GZIP(
                GzipLevel::try_new(level as u32).unwrap_or_default(),
            ),
            "LZ4" => Compression::LZ4,
            "BROTLI" => Compression::BROTLI(
                BrotliLevel::try_new(level as u32).unwrap_or_default(),
            ),
            "LZ4_RAW" => Compression::LZ4_RAW,
            "UNCOMPRESSED" => Compression::UNCOMPRESSED,
            _ => Compression::ZSTD(ZstdLevel::try_new(level).unwrap_or(ZstdLevel::default())),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::native_settings::NativeSettings;

    #[test]
    fn test_build_with_compression() {
        let config = NativeSettings {
            compression_type: Some("ZSTD".to_string()),
            compression_level: Some(5),
            ..Default::default()
        };
        let props = WriterPropertiesBuilder::build(&config);
        assert_ne!(
            props.compression(&parquet::schema::types::ColumnPath::from("test")),
            Compression::UNCOMPRESSED
        );
    }

    #[test]
    fn test_parse_compression_types() {
        assert!(matches!(
            WriterPropertiesBuilder::parse_compression_type("ZSTD", 3),
            Compression::ZSTD(_)
        ));
        assert!(matches!(
            WriterPropertiesBuilder::parse_compression_type("SNAPPY", 0),
            Compression::SNAPPY
        ));
        assert!(matches!(
            WriterPropertiesBuilder::parse_compression_type("GZIP", 6),
            Compression::GZIP(_)
        ));
    }
}
