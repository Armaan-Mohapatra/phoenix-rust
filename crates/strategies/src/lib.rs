//! Frozen strategy specifications and offline model-signal selection.
//! Not a strategy optimizer. Every decision, including abstention, is retained.
pub mod frozen_registry;
use phoenix_domain::{require_asof,Book,FeeRate,Price,Side};
use phoenix_research::{choose_trade,SelectMode,TradeChoice};

#[derive(Debug,Clone,PartialEq,Eq)]
pub enum Error {MissingVersion,InvalidHash,UnknownModel,FutureSignal,InvalidWeights,
    MissingQuote,InvalidBook}
#[derive(Debug,Clone,Copy,PartialEq,Eq)]
pub enum Recipe {NoTrade,MarketImplied,ModelEdge,InvertedModelEdge,WeightedEnsemble}
#[derive(Debug,Clone)]
pub struct FrozenSpec {
    pub id:String,pub code_sha256:String,pub config_sha256:String,
    pub recipe:Recipe,pub min_net_edge_ticks:i64,pub max_quote_age_ns:i64,
    pub fee_rate:FeeRate,pub max_spread_ticks:Option<u16>,
}
impl FrozenSpec {
    pub fn validate(&self)->Result<(),Error>{
        if self.id.is_empty(){return Err(Error::MissingVersion);}
        let hex_ok=|v:&str|v.len()==64&&v.bytes().all(|b|b.is_ascii_hexdigit());
        if !hex_ok(&self.code_sha256)||!hex_ok(&self.config_sha256){
            return Err(Error::InvalidHash);
        }
        if self.min_net_edge_ticks<0||self.max_quote_age_ns<0{
            return Err(Error::InvalidWeights);
        }
        Ok(())
    }
}
#[derive(Debug,Clone,Copy)]
pub struct ModelSignal {
    pub asof_received_ns:i64,pub probability_yes:Price,
    pub model_weight:u32,
}
#[derive(Debug,Clone,Copy,PartialEq,Eq)]
pub enum Abstain {NoTradeControl,NoValidQuote,StaleOrFutureQuote,
    MissingSignal,UnprofitableAfterCosts,SpreadTooWide}
#[derive(Debug,Clone,Copy,PartialEq,Eq)]
pub enum Decision {Trade(TradeChoice),Abstain(Abstain)}
fn ensemble(models:&[ModelSignal])->Result<Price,Error>{
    if models.is_empty()||models.len()>16{return Err(Error::UnknownModel);}
    let total:u128=models.iter().map(|m|u128::from(m.model_weight)).sum();
    if total==0{return Err(Error::InvalidWeights);}
    let weighted:u128=models.iter()
        .map(|m|u128::from(m.probability_yes.ticks())*u128::from(m.model_weight)).sum();
    let ticks=u16::try_from(weighted/total).map_err(|_|Error::InvalidWeights)?;
    Price::new(ticks).map_err(|_|Error::InvalidWeights)
}
/// Model signals and book must have arrived by decision time; freshness cap
/// applies separately. No writes, statistical fitting, API requests or orders.
pub fn evaluate(spec:&FrozenSpec,decision_ns:i64,book:&Book,models:&[ModelSignal])
    ->Result<Decision,Error>{
    spec.validate()?;
    if spec.recipe==Recipe::NoTrade{return Ok(Decision::Abstain(Abstain::NoTradeControl));}
    book.validate().map_err(|_|Error::InvalidBook)?;
    if require_asof(book.received_ns,decision_ns,spec.max_quote_age_ns).is_err(){
        return Ok(Decision::Abstain(Abstain::StaleOrFutureQuote));
    }
    let (yes,no)=(book.best_ask(Side::Yes),book.best_ask(Side::No));
    if yes.is_none()||no.is_none(){return Ok(Decision::Abstain(Abstain::NoValidQuote));}
    if let Some(cap)=spec.max_spread_ticks{
        if book.spread(Side::Yes).is_some_and(|s|s>cap){
            return Ok(Decision::Abstain(Abstain::SpreadTooWide));
        }
    }
    if models.iter().any(|m|require_asof(m.asof_received_ns,decision_ns,
        spec.max_quote_age_ns).is_err()){
        return Ok(Decision::Abstain(Abstain::StaleOrFutureQuote));
    }
    let implied=||{
        let bid=book.best_bid(Side::Yes)?.ticks();
        Some(Price::new(((u32::from(bid)+u32::from(yes?.ticks()))/2) as u16).ok()?)
    };
    let q=match spec.recipe {
        Recipe::MarketImplied=>implied().ok_or(Error::MissingQuote)?,
        Recipe::ModelEdge|Recipe::InvertedModelEdge=>{
            models.first().ok_or(Error::UnknownModel)?.probability_yes
        }
        Recipe::WeightedEnsemble=>ensemble(models)?,
        Recipe::NoTrade=>unreachable!(),
    };
    let mode=if spec.recipe==Recipe::InvertedModelEdge{
        SelectMode::InvertScore
    }else{SelectMode::FairValue};
    let choice=choose_trade(q,yes,no,spec.fee_rate,spec.min_net_edge_ticks,mode);
    Ok(choice.map_or(Decision::Abstain(Abstain::UnprofitableAfterCosts),Decision::Trade))
}
#[cfg(test)]
mod tests {
    use super::*;use phoenix_domain::Level;
    fn p(x:u16)->Price {Price::new(x).unwrap()}
    fn spec(recipe:Recipe)->FrozenSpec {FrozenSpec{id:"example-v1".into(),
        code_sha256:"a".repeat(64),config_sha256:"b".repeat(64),recipe,
        min_net_edge_ticks:200,max_quote_age_ns:1_000,
        fee_rate:FeeRate::STANDARD_TAKER,max_spread_ticks:Some(1_000)}}
    fn book()->Book {Book{market:"BTC".into(),
        yes_bids:vec![Level{price:p(4000),quantity:10}],
        no_bids:vec![Level{price:p(5500),quantity:10}],
        exchange_ns:90,received_ns:100,session_id:"a".into(),sequence:1}}
    #[test]fn no_trade_baseline_never_trades(){
        assert_eq!(evaluate(&spec(Recipe::NoTrade),120,&book(),&[]).unwrap(),
            Decision::Abstain(Abstain::NoTradeControl));}
    #[test]fn model_edge_must_clear_all_costs(){
        let m=ModelSignal{asof_received_ns:110,probability_yes:p(6500),model_weight:1};
        assert!(matches!(evaluate(&spec(Recipe::ModelEdge),120,&book(),&[m]).unwrap(),
            Decision::Trade(TradeChoice{side:Side::Yes,..})));
        let bad=ModelSignal{probability_yes:p(4550),..m};
        assert_eq!(evaluate(&spec(Recipe::ModelEdge),120,&book(),&[bad]).unwrap(),
            Decision::Abstain(Abstain::UnprofitableAfterCosts));}
    #[test]fn future_predictions_are_rejected(){
        let m=ModelSignal{asof_received_ns:121,probability_yes:p(6500),model_weight:1};
        assert_eq!(evaluate(&spec(Recipe::ModelEdge),120,&book(),&[m]).unwrap(),
            Decision::Abstain(Abstain::StaleOrFutureQuote));}
    #[test]fn weighted_models_and_inverse_are_distinct(){
        let models=[ModelSignal{asof_received_ns:101,probability_yes:p(7000),model_weight:3},
            ModelSignal{asof_received_ns:102,probability_yes:p(3000),model_weight:1}];
        let e=ensemble(&models).unwrap();
        assert_eq!(e,p(6000));
        assert!(matches!(evaluate(&spec(Recipe::WeightedEnsemble),120,&book(),&models),
            Ok(Decision::Trade(_))));
        assert!(matches!(evaluate(&spec(Recipe::InvertedModelEdge),120,&book(),&models),
            Ok(Decision::Trade(_))|Ok(Decision::Abstain(_))));}
    #[test]fn invalid_version_fails_closed(){
        let mut s=spec(Recipe::ModelEdge);s.config_sha256="incorrect".into();
        assert_eq!(evaluate(&s,120,&book(),&[]),Err(Error::InvalidHash));}
}
