use std::borrow::Cow;
use std::convert::identity;
use std::mem::size_of;

use anyhow::format_err;
use safe_transmute::{
    SingleManyGuard, transmute_many, transmute_many_pedantic, transmute_one_pedantic,
    transmute_one_to_bytes, transmute_to_bytes,
};
use serum_dex::matching::Side;
use serum_dex::state::{
    AccountFlag, Event, EventQueueHeader, EventView, gen_vault_signer_key, Market, MarketState,
    MarketStateV2, QueueHeader,
};
use solana_client::rpc_client::RpcClient;
use solana_sdk::commitment_config::CommitmentConfig;
use solana_sdk::pubkey::Pubkey;

#[derive(Debug)]
pub struct MarketPubkeys {
    pub market: Box<Pubkey>,
    pub req_q: Box<Pubkey>,
    pub event_q: Box<Pubkey>,
    pub bids: Box<Pubkey>,
    pub asks: Box<Pubkey>,
    pub coin_vault: Box<Pubkey>,
    pub pc_vault: Box<Pubkey>,
    pub vault_signer_key: Box<Pubkey>,
}

pub struct EventQueue {
    pub header: EventQueueHeader,
    pub events: Vec<EventView>,
}

pub fn load_event_queue(client: &RpcClient, dex_program_id: &Pubkey, market: &Pubkey) -> anyhow::Result<()> {
    let market_keys = get_keys_for_market(&client, dex_program_id, &market)?;
    let event_q_data = client.get_account_data(&market_keys.event_q)?;
    let inner: Cow<[u64]> = remove_dex_account_padding(&event_q_data)?;
    let event_queue = parse_event_queue(&inner)?;
    Ok(())
}

fn parse_event_queue(data_words: &[u64]) -> anyhow::Result<EventQueue> {
    let (header_words, event_words) = data_words.split_at(size_of::<EventQueueHeader>() >> 3);
    let header: EventQueueHeader =
        transmute_one_pedantic(transmute_to_bytes(header_words)).map_err(|e| e.without_src())?;
    let events: &[Event] = transmute_many::<_, SingleManyGuard>(transmute_to_bytes(event_words))
        .map_err(|e| e.without_src())?;
    let (_, head_seg) = events.split_at(header.head() as usize);
    let head_len = head_seg.len().min(header.count() as usize);

    Ok((EventQueue {
        header,
        events: Vec::from(&head_seg[..head_len].map(|e| e.as_view()?))
    }))
}

// fn parse_event(event: Event) -> anyhow::Result<()> {
//     match event.as_view()? {
//         EventView::Fill {
//             side,
//             maker,
//             native_qty_paid,
//             native_qty_received,
//             native_fee_or_rebate,
//             fee_tier: _,
//             order_id: _,
//             owner: _,
//             owner_slot,
//             client_order_id,
//         } => {
//             native_qty_paid
//                 .checked_add(native_fee_or_rebate)
//                 .ok_or()
//             let mut price = if maker {
//                 native_qty_paid + native_fee_or_rebate
//             } else {
//                 native_qty_paid - native_fee_or_rebate
//             };
//             match side {
//                 Side::Bid => {
//                     price =
//                 }
//                 Side::Ask => {
//                 }
//             }
//         },
//         EventView::Out {
//                side,
//                release_funds,
//                native_qty_unlocked,
//                native_qty_still_locked,
//                order_id: _,
//                owner: _,
//                owner_slot,
//                client_order_id,
//            } => {
//         }
//     };
//     Ok(())
// }

#[cfg(target_endian = "little")]
fn remove_dex_account_padding<'a>(data: &'a [u8]) -> anyhow::Result<Cow<'a, [u64]>> {
    use serum_dex::state::{ACCOUNT_HEAD_PADDING, ACCOUNT_TAIL_PADDING};
    let head = &data[..ACCOUNT_HEAD_PADDING.len()];
    if data.len() < ACCOUNT_HEAD_PADDING.len() + ACCOUNT_TAIL_PADDING.len() {
        return Err(format_err!(
            "dex account length {} is too small to contain valid padding",
            data.len()
        ));
    }
    if head != ACCOUNT_HEAD_PADDING {
        return Err(format_err!("dex account head padding mismatch"));
    }
    let tail = &data[data.len() - ACCOUNT_TAIL_PADDING.len()..];
    if tail != ACCOUNT_TAIL_PADDING {
        return Err(format_err!("dex account tail padding mismatch"));
    }
    let inner_data_range = ACCOUNT_HEAD_PADDING.len()..(data.len() - ACCOUNT_TAIL_PADDING.len());
    let inner: &'a [u8] = &data[inner_data_range];
    let words: Cow<'a, [u64]> = match transmute_many_pedantic::<u64>(inner) {
        Ok(word_slice) => Cow::Borrowed(word_slice),
        Err(transmute_error) => {
            let word_vec = transmute_error.copy().map_err(|e| e.without_src())?;
            Cow::Owned(word_vec)
        }
    };
    Ok(words)
}

#[cfg(target_endian = "little")]
fn get_keys_for_market<'a>(
    client: &'a RpcClient,
    program_id: &'a Pubkey,
    market: &'a Pubkey,
) -> anyhow::Result<MarketPubkeys> {
    let account_data: Vec<u8> = client.get_account_data(&market)?;
    let words: Cow<[u64]> = remove_dex_account_padding(&account_data)?;
    let market_state: MarketState = {
        let account_flags = Market::account_flags(&account_data)?;
        if account_flags.intersects(AccountFlag::Permissioned) {
            let state = transmute_one_pedantic::<MarketStateV2>(transmute_to_bytes(&words))
                .map_err(|e| e.without_src())?;
            state.check_flags(true)?;
            state.inner
        } else {
            let state = transmute_one_pedantic::<MarketState>(transmute_to_bytes(&words))
                .map_err(|e| e.without_src())?;
            state.check_flags(true)?;
            state
        }
    };
    let vault_signer_key =
        gen_vault_signer_key(market_state.vault_signer_nonce, market, program_id)?;
    assert_eq!(
        transmute_to_bytes(&identity(market_state.own_address)),
        market.as_ref()
    );
    Ok(MarketPubkeys {
        market: Box::new(*market),
        req_q: Box::new(Pubkey::new(transmute_one_to_bytes(&identity(
            market_state.req_q,
        )))),
        event_q: Box::new(Pubkey::new(transmute_one_to_bytes(&identity(
            market_state.event_q,
        )))),
        bids: Box::new(Pubkey::new(transmute_one_to_bytes(&identity(
            market_state.bids,
        )))),
        asks: Box::new(Pubkey::new(transmute_one_to_bytes(&identity(
            market_state.asks,
        )))),
        coin_vault: Box::new(Pubkey::new(transmute_one_to_bytes(&identity(
            market_state.coin_vault,
        )))),
        pc_vault: Box::new(Pubkey::new(transmute_one_to_bytes(&identity(
            market_state.pc_vault,
        )))),
        vault_signer_key: Box::new(vault_signer_key),
    })
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::*;

    #[test]
    fn load_sol_usdc() -> anyhow::Result<()> {
        let client = RpcClient::new_with_commitment(
            "https://solana-api.projectserum.com".to_string(),
            CommitmentConfig::confirmed(),
        );

        load_event_queue(
            &client,
            &Pubkey::from_str("9xQeWvG816bUx9EPjHmaT23yvVM2ZWbrrpZb9PusVFin")?,
            &Pubkey::from_str("6oGsL2puUgySccKzn9XA9afqF217LfxP5ocq4B3LWsjy")?,
        )
    }
}
