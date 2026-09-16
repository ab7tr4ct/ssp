use arrow::{
    array::{BinaryBuilder, BooleanBuilder, RecordBatch, UInt8Builder, UInt64Builder},
    datatypes::{DataType, Field, Schema},
};
use foldhash::HashSet;
use std::sync::Arc;

use super::{BATCH_THRESHOLD, Mint, TOKEN_PROGRAM};
use crate::Pubkey;

pub struct MintDecoder {
    schema: Arc<Schema>,
    rows: usize,
    known_mints: Arc<HashSet<Pubkey>>,
    pubkey_b: BinaryBuilder,
    mint_authority_b: BinaryBuilder,
    freeze_authority_b: BinaryBuilder,
    supply_b: UInt64Builder,
    decimals_b: UInt8Builder,
    is_initialized_b: BooleanBuilder,
}

impl MintDecoder {
    pub fn new(known_mints: Arc<HashSet<Pubkey>>) -> Self {
        Self {
            known_mints,
            schema: Arc::new(Schema::new(vec![
                Field::new("pubkey", DataType::Binary, false),
                Field::new("mint_authority", DataType::Binary, true),
                Field::new("freeze_authority", DataType::Binary, true),
                Field::new("supply", DataType::UInt64, false),
                Field::new("decimals", DataType::UInt8, false),
                Field::new("is_initialized", DataType::Boolean, false),
            ])),
            rows: 0,
            pubkey_b: BinaryBuilder::with_capacity(BATCH_THRESHOLD, BATCH_THRESHOLD * 32),
            mint_authority_b: BinaryBuilder::with_capacity(BATCH_THRESHOLD, BATCH_THRESHOLD * 32),
            freeze_authority_b: BinaryBuilder::with_capacity(BATCH_THRESHOLD, BATCH_THRESHOLD * 32),
            supply_b: UInt64Builder::with_capacity(BATCH_THRESHOLD),
            decimals_b: UInt8Builder::with_capacity(BATCH_THRESHOLD),
            is_initialized_b: BooleanBuilder::with_capacity(BATCH_THRESHOLD),
        }
    }

    fn build_batch(&mut self) -> Option<RecordBatch> {
        if self.rows == 0 {
            return None;
        }
        self.rows = 0;

        let batch = RecordBatch::try_new(
            self.schema.clone(),
            vec![
                Arc::new(self.pubkey_b.finish()),
                Arc::new(self.mint_authority_b.finish()),
                Arc::new(self.freeze_authority_b.finish()),
                Arc::new(self.supply_b.finish()),
                Arc::new(self.decimals_b.finish()),
                Arc::new(self.is_initialized_b.finish()),
            ],
        )
        .ok();

        self.pubkey_b = BinaryBuilder::with_capacity(BATCH_THRESHOLD, BATCH_THRESHOLD * 32);
        self.mint_authority_b = BinaryBuilder::with_capacity(BATCH_THRESHOLD, BATCH_THRESHOLD * 32);
        self.freeze_authority_b = BinaryBuilder::with_capacity(BATCH_THRESHOLD, BATCH_THRESHOLD * 32);
        self.supply_b = UInt64Builder::with_capacity(BATCH_THRESHOLD);
        self.decimals_b = UInt8Builder::with_capacity(BATCH_THRESHOLD);
        self.is_initialized_b = BooleanBuilder::with_capacity(BATCH_THRESHOLD);

        batch
    }
}

impl crate::decoders::Decoder for MintDecoder {
    fn name(&self) -> &'static str {
        "mints"
    }

    fn owner(&self) -> Pubkey {
        TOKEN_PROGRAM
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn matches(&self, owner: &Pubkey, data_len: u64) -> bool {
        owner == &TOKEN_PROGRAM && data_len == Mint::SIZE as u64
    }

    fn decode(&mut self, pubkey: Pubkey, data: &[u8], include_spam: bool) -> Option<RecordBatch> {
        if !include_spam && !self.known_mints.contains(&pubkey) {
            return None;
        }

        let mint = bytemuck::from_bytes::<Mint>(data);

        self.pubkey_b.append_value(pubkey);

        match mint.mint_authority.get() {
            Some(pk) => self.mint_authority_b.append_value(pk),
            None => self.mint_authority_b.append_null(),
        }
        match mint.freeze_authority.get() {
            Some(pk) => self.freeze_authority_b.append_value(pk),
            None => self.freeze_authority_b.append_null(),
        }

        self.supply_b.append_value(mint.supply);
        self.decimals_b.append_value(mint.decimals);
        self.is_initialized_b.append_value(mint.is_initialized != 0);

        self.rows += 1;

        if self.rows >= BATCH_THRESHOLD {
            self.build_batch()
        } else {
            None
        }
    }

    fn flush(&mut self) -> Option<RecordBatch> {
        self.build_batch()
    }
}
