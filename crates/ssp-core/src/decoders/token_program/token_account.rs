use arrow::{
    array::{BinaryBuilder, RecordBatch, UInt8Builder, UInt64Builder},
    datatypes::{DataType, Field, Schema},
};
use foldhash::HashSet;
use std::sync::Arc;

use super::{BATCH_THRESHOLD, TOKEN_PROGRAM, TokenAccount};
use crate::Pubkey;

pub struct TokenAccountDecoder {
    pub schema: Arc<Schema>,
    pub rows: usize,
    known_mints: Arc<HashSet<Pubkey>>,
    pub pubkey_b: BinaryBuilder,
    pub mint_b: BinaryBuilder,
    pub owner_b: BinaryBuilder,
    pub amount_b: UInt64Builder,
    pub delegate_b: BinaryBuilder,
    pub state_b: UInt8Builder,
    pub is_native_b: UInt64Builder,
    pub delegated_amount_b: UInt64Builder,
    pub close_authority_b: BinaryBuilder,
}

impl TokenAccountDecoder {
    fn build_batch(&mut self) -> Option<RecordBatch> {
        if self.rows == 0 {
            return None;
        }
        self.rows = 0;

        let batch = RecordBatch::try_new(
            self.schema.clone(),
            vec![
                Arc::new(self.pubkey_b.finish()),
                Arc::new(self.mint_b.finish()),
                Arc::new(self.owner_b.finish()),
                Arc::new(self.amount_b.finish()),
                Arc::new(self.delegate_b.finish()),
                Arc::new(self.state_b.finish()),
                Arc::new(self.is_native_b.finish()),
                Arc::new(self.delegated_amount_b.finish()),
                Arc::new(self.close_authority_b.finish()),
            ],
        )
        .ok();

        self.pubkey_b = BinaryBuilder::with_capacity(BATCH_THRESHOLD, BATCH_THRESHOLD * 32);
        self.mint_b = BinaryBuilder::with_capacity(BATCH_THRESHOLD, BATCH_THRESHOLD * 32);
        self.owner_b = BinaryBuilder::with_capacity(BATCH_THRESHOLD, BATCH_THRESHOLD * 32);
        self.amount_b = UInt64Builder::with_capacity(BATCH_THRESHOLD);
        self.delegate_b = BinaryBuilder::with_capacity(BATCH_THRESHOLD, BATCH_THRESHOLD * 32);
        self.state_b = UInt8Builder::with_capacity(BATCH_THRESHOLD);
        self.is_native_b = UInt64Builder::with_capacity(BATCH_THRESHOLD);
        self.delegated_amount_b = UInt64Builder::with_capacity(BATCH_THRESHOLD);
        self.close_authority_b =
            BinaryBuilder::with_capacity(BATCH_THRESHOLD, BATCH_THRESHOLD * 32);

        batch
    }
}

impl crate::decoders::Decoder for TokenAccountDecoder {
    fn name(&self) -> &'static str {
        "token_accounts"
    }

    fn owner(&self) -> Pubkey {
        TOKEN_PROGRAM
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn matches(&self, owner: &Pubkey, data_len: u64) -> bool {
        owner == &TOKEN_PROGRAM && data_len == TokenAccount::SIZE as u64
    }

    fn decode(&mut self, pubkey: Pubkey, data: &[u8], include_spam: bool) -> Option<RecordBatch> {
        let acc = bytemuck::from_bytes::<TokenAccount>(data);

        if !include_spam && !self.known_mints.contains(&acc.mint) {
            return None;
        }

        self.pubkey_b.append_value(pubkey);
        self.mint_b.append_value(acc.mint);
        self.owner_b.append_value(acc.owner);
        self.amount_b.append_value(acc.amount);

        match acc.delegate.get() {
            Some(pk) => self.delegate_b.append_value(pk),
            None => self.delegate_b.append_null(),
        }

        self.state_b.append_value(acc.state);

        match acc.is_native.get() {
            Some(v) => self.is_native_b.append_value(v),
            None => self.is_native_b.append_null(),
        }

        self.delegated_amount_b.append_value(acc.delegated_amount);

        match acc.close_authority.get() {
            Some(pk) => self.close_authority_b.append_value(pk),
            None => self.close_authority_b.append_null(),
        }

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

impl TokenAccountDecoder {
    pub fn new(known_mints: Arc<HashSet<Pubkey>>) -> Self {
        Self {
            schema: Arc::new(Schema::new(vec![
                Field::new("pubkey", DataType::Binary, false),
                Field::new("mint", DataType::Binary, false),
                Field::new("owner", DataType::Binary, false),
                Field::new("amount", DataType::UInt64, false),
                Field::new("delegate", DataType::Binary, true),
                Field::new("state", DataType::UInt8, false),
                Field::new("is_native", DataType::UInt64, true),
                Field::new("delegated_amount", DataType::UInt64, false),
                Field::new("close_authority", DataType::Binary, true),
            ])),

            known_mints,
            rows: 0,
            pubkey_b: BinaryBuilder::with_capacity(BATCH_THRESHOLD, BATCH_THRESHOLD * 32),
            mint_b: BinaryBuilder::with_capacity(BATCH_THRESHOLD, BATCH_THRESHOLD * 32),
            owner_b: BinaryBuilder::with_capacity(BATCH_THRESHOLD, BATCH_THRESHOLD * 32),
            amount_b: UInt64Builder::with_capacity(BATCH_THRESHOLD),
            delegate_b: BinaryBuilder::with_capacity(BATCH_THRESHOLD, BATCH_THRESHOLD * 32),
            state_b: UInt8Builder::with_capacity(BATCH_THRESHOLD),
            is_native_b: UInt64Builder::with_capacity(BATCH_THRESHOLD),
            delegated_amount_b: UInt64Builder::with_capacity(BATCH_THRESHOLD),
            close_authority_b: BinaryBuilder::with_capacity(BATCH_THRESHOLD, BATCH_THRESHOLD * 32),
        }
    }
}
