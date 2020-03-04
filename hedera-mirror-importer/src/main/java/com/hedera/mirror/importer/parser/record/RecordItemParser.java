package com.hedera.mirror.importer.parser.record;

/*-
 * ‌
 * Hedera Mirror Node
 * ​
 * Copyright (C) 2019 - 2020 Hedera Hashgraph, LLC
 * ​
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * ‍
 */

import com.google.protobuf.InvalidProtocolBufferException;
import com.hederahashgraph.api.proto.java.AccountAmount;
import com.hederahashgraph.api.proto.java.AccountID;
import com.hederahashgraph.api.proto.java.ConsensusSubmitMessageTransactionBody;
import com.hederahashgraph.api.proto.java.ContractCallTransactionBody;
import com.hederahashgraph.api.proto.java.ContractCreateTransactionBody;
import com.hederahashgraph.api.proto.java.ContractFunctionResult;
import com.hederahashgraph.api.proto.java.ContractID;
import com.hederahashgraph.api.proto.java.CryptoAddClaimTransactionBody;
import com.hederahashgraph.api.proto.java.CryptoCreateTransactionBody;
import com.hederahashgraph.api.proto.java.FileAppendTransactionBody;
import com.hederahashgraph.api.proto.java.FileCreateTransactionBody;
import com.hederahashgraph.api.proto.java.FileID;
import com.hederahashgraph.api.proto.java.FileUpdateTransactionBody;
import com.hederahashgraph.api.proto.java.TopicID;
import com.hederahashgraph.api.proto.java.Transaction;
import com.hederahashgraph.api.proto.java.TransactionBody;
import com.hederahashgraph.api.proto.java.TransactionReceipt;
import com.hederahashgraph.api.proto.java.TransactionRecord;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.inject.Named;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.codec.binary.Base64;

import com.hedera.mirror.importer.addressbook.NetworkAddressBook;
import com.hedera.mirror.importer.exception.ImporterException;
import com.hedera.mirror.importer.exception.ParserException;
import com.hedera.mirror.importer.parser.domain.RecordItem;
import com.hedera.mirror.importer.util.Utility;

@Log4j2
@Named
public class RecordItemParser implements RecordItemListener {
    private final static Map<String, Object> EMPTY_MAP = new HashMap<>();

    private final RecordParserProperties parserProperties;
    private final NetworkAddressBook networkAddressBook;
    private final NonFeeTransferExtractionStrategy nonFeeTransfersExtractor;
    private final BigQueryWriter bigQueryWriter;

    public RecordItemParser(RecordParserProperties parserProperties,
                            NetworkAddressBook networkAddressBook,
                            NonFeeTransferExtractionStrategy nonFeeTransfersExtractor,
                            BigQueryWriter bigQueryWriter) {
        this.parserProperties = parserProperties;
        this.networkAddressBook = networkAddressBook;
        this.nonFeeTransfersExtractor = nonFeeTransfersExtractor;
        this.bigQueryWriter = bigQueryWriter;
    }

    @Override
    public void onItem(RecordItem recordItem) throws ImporterException {
        Transaction transaction = recordItem.getTransaction();
        TransactionBody body = getTransactionBody(transaction);
        TransactionRecord txRecord = recordItem.getRecord();
        // TODO: note that filtering not supported. Or support it.

        log.trace("Storing transaction body: {}", () -> Utility.printProtoMessage(body));
        Map<String, Object> bqRow = new HashMap<>();
        long consensusTimestamp = Utility.timeStampInNanos(txRecord.getConsensusTimestamp());
        bqRow.put("consensusTimestamp", consensusTimestamp);
        bqRow.put("transactionType", getTransactionType(body));
        addTransactionBodyFields(bqRow, body, txRecord);
        addTransactionRecordFields(bqRow, txRecord);
        addNonFeeTransfers(bqRow, body, txRecord);
        log.debug("Storing row: {}", bqRow);
        bigQueryWriter.insertRow(String.valueOf(consensusTimestamp), bqRow);
    }

    private void addTransactionBodyFields(Map<String, Object> bqRow, TransactionBody body, TransactionRecord txRecord) {
        Map<String, Object> bodyFields = new HashMap<>();
        bqRow.put("transaction", Map.of("body", bodyFields));
        bodyFields.put("transactionID", Map.of(
                "transactionValidStart", Utility.timeStampInNanos(body.getTransactionID().getTransactionValidStart()),
                "accountID", makeEntityMap(body.getTransactionID().getAccountID())));
        bodyFields.put("nodeAccountID", makeEntityMap(body.getNodeAccountID()));
        bodyFields.put("transactionFee", body.getTransactionFee());
        bodyFields.put("transactionValidDuration", Map.of("seconds", body.getTransactionValidDuration().getSeconds()));
        bodyFields.put("memo", body.getMemo());
        addTransactionBodyDataFields(bqRow, body, txRecord);
    }

    private void addTransactionBodyDataFields(
            Map<String, Object> bqRow, TransactionBody body, TransactionRecord txRecord) {
        if (body.hasContractCall()) {
            addContractCallFields(bqRow, body.getContractCall());
        } else if (body.hasContractCreateInstance()) {
            addContractCreateFields(bqRow, body.getContractCreateInstance());
        } else if (body.hasContractDeleteInstance()) {
            bqRow.putAll(getCudEntityBq(body.getContractDeleteInstance().getContractID()));
        } else if (body.hasContractUpdateInstance()) {
            bqRow.putAll(getCudEntityBq(body.getContractUpdateInstance().getContractID()));
        } else if (body.hasCryptoCreateAccount()) {
            addCryptoCreateAccountFields(bqRow, body.getCryptoCreateAccount());
        } else if (body.hasCryptoAddClaim()) {
            addCryptoAddClaimFields(bqRow, body.getCryptoAddClaim());
        } else if (body.hasCryptoDelete()) {
            bqRow.putAll(getCudEntityBq(body.getCryptoDelete().getDeleteAccountID()));
        } else if (body.hasCryptoDeleteClaim()) {
            bqRow.putAll(getCudEntityBq(body.getCryptoDeleteClaim().getAccountIDToDeleteFrom()));
        } else if (body.hasCryptoUpdateAccount()) {
            bqRow.putAll(getCudEntityBq(body.getCryptoUpdateAccount().getAccountIDToUpdate()));
        } else if (body.hasFileCreate()) {
            addFileCreateFields(bqRow, body.getFileCreate(), txRecord.getReceipt().getFileID());
        } else if (body.hasFileAppend()) {
            addFileAppendFields(bqRow, body.getFileAppend());
        } else if (body.hasFileDelete()) {
            bqRow.putAll(getCudEntityBq(body.getFileDelete().getFileID()));
        } else if (body.hasFileUpdate()) {
            addFileUpdateFields(bqRow, body.getFileUpdate());
        } else if (body.hasSystemDelete()) {
            if (body.getSystemDelete().hasContractID()) {
                bqRow.putAll(getCudEntityBq(body.getSystemDelete().getContractID()));
            } else if (body.getSystemDelete().hasFileID()) {
                bqRow.putAll(getCudEntityBq(body.getSystemDelete().getFileID()));
            }
        } else if (body.hasSystemUndelete()) {
            if (body.getSystemUndelete().hasContractID()) {
                bqRow.putAll(getCudEntityBq(body.getSystemUndelete().getContractID()));
            } else if (body.getSystemUndelete().hasFileID()) {
                bqRow.putAll(getCudEntityBq(body.getSystemUndelete().getFileID()));
            }
        } else if (body.hasConsensusCreateTopic()) {
            // do nothing
        } else if (body.hasConsensusUpdateTopic()) {
            bqRow.putAll(getCudEntityBq(body.getConsensusUpdateTopic().getTopicID()));
        } else if (body.hasConsensusDeleteTopic()) {
            bqRow.putAll(getCudEntityBq(body.getConsensusDeleteTopic().getTopicID()));
        } else if (body.hasConsensusSubmitMessage()) {
            addConsensusSubmitMessageFields(bqRow, body.getConsensusSubmitMessage());
        }
    }

    private void addTransactionRecordFields(Map<String, Object> bqRow, TransactionRecord txRecord) {
        Map<String, Object> recordFields = new HashMap<>();
        addTransactionReceiptFields(bqRow, recordFields, txRecord.getReceipt());
        recordFields.put("transactionHash", Base64.encodeBase64String(txRecord.getTransactionHash().toByteArray()));
        recordFields.put("transactionFee", txRecord.getTransactionFee());
        if (txRecord.hasContractCallResult()) {
            recordFields.put("contractCallResult", getContractFunctionResultFields(txRecord.getContractCallResult()));
        }
        if (txRecord.hasContractCreateResult()) {
            recordFields.put("contractCreateResult",
                    getContractFunctionResultFields(txRecord.getContractCreateResult()));
        }
        if ((txRecord.hasTransferList()) && parserProperties.getPersist().isCryptoTransferAmounts()) {
            recordFields.put("transferList",
                    getTransferListFields(txRecord.getTransferList().getAccountAmountsList().iterator()));
        }
        bqRow.put("record", recordFields);
    }

    private void addTransactionReceiptFields(
            Map<String, Object> bqRow, Map<String, Object> recordFields, TransactionReceipt txReceipt) {
        Map<String, Object> receiptFields = new HashMap<>();
        receiptFields.put("status", txReceipt.getStatus().getNumber());
        if (txReceipt.hasAccountID()) {
            bqRow.putAll(getCudEntityBq(txReceipt.getAccountID()));
        } else if (txReceipt.hasFileID()) {
            bqRow.putAll(getCudEntityBq(txReceipt.getFileID()));
        } else if (txReceipt.hasContractID()) {
            bqRow.putAll(getCudEntityBq(txReceipt.getContractID()));
        } else if (txReceipt.hasTopicID()) {
            bqRow.putAll(getCudEntityBq(txReceipt.getTopicID()));
        }
        if (txReceipt.getTopicSequenceNumber() != 0) {
            receiptFields.put("topicSequenceNumber", txReceipt.getTopicSequenceNumber());
        }
        if (!txReceipt.getTopicRunningHash().isEmpty()) {
            receiptFields.put("topicRunningHash",
                    Base64.encodeBase64String(txReceipt.getTopicRunningHash().toByteArray()));
        }
        recordFields.put("receipt", receiptFields);
    }

    /**
     * Should the given transaction/record generate non_fee_transfers based on what type the transaction is, it's
     * status, and run-time configuration concerning which situations warrant storing.
     */
    private boolean shouldStoreNonFeeTransfers(TransactionBody body) {
        if (!body.hasCryptoCreateAccount() && !body.hasContractCreateInstance() && !body.hasCryptoTransfer() &&
                !body.hasContractCall()) {
            return false;
        }
        return parserProperties.getPersist().isCryptoTransferAmounts()
                && parserProperties.getPersist().isNonFeeTransfers();
    }

    /**
     * Additionally store rows in the non_fee_transactions table if applicable. This will allow the rest-api to create
     * an itemized set of transfers that reflects non-fees (explicit transfers), threshold records, node fee, and
     * network+service fee (paid to treasury).
     */
    private void addNonFeeTransfers(
            Map<String, Object> bqRow, TransactionBody body, TransactionRecord transactionRecord) {
        if (transactionRecord.hasTransferList() && shouldStoreNonFeeTransfers(body)) {
            bqRow.put("nonFeeTransfers", getTransferListFields(nonFeeTransfersExtractor.extractNonFeeTransfers(
                    body.getTransactionID().getAccountID(), body, transactionRecord).iterator()));
        }
    }

    private void addConsensusSubmitMessageFields(
            Map<String, Object> bqRow, ConsensusSubmitMessageTransactionBody submitMessage) {
        bqRow.putAll(getCudEntityBq(submitMessage.getTopicID()));
        getBodyMap(bqRow).put("consensusSubmitMessage",
                Map.of("message", Base64.encodeBase64String(submitMessage.getMessage().toByteArray())));
    }

    private void addFileCreateFields(Map<String, Object> bqRow, FileCreateTransactionBody fileCreate,
                                     FileID fileId) {
        if (parserProperties.getPersist().isFiles() ||
                (parserProperties.getPersist().isSystemFiles() && fileId.getFileNum() < 1000)) {
            getBodyMap(bqRow).put("fileCreate", Map.of("contents",
                    Base64.encodeBase64String(fileCreate.getContents().toByteArray())));
        }
    }

    private void addFileAppendFields(Map<String, Object> bqRow, FileAppendTransactionBody fileAppend) {
        FileID fileId = fileAppend.getFileID();
        bqRow.putAll(getCudEntityBq(fileId));
        byte[] contents = fileAppend.getContents().toByteArray();
        if (parserProperties.getPersist().isFiles() ||
                (parserProperties.getPersist().isSystemFiles() && fileId.getFileNum() < 1000)) {
            getBodyMap(bqRow).put("fileAppend", Map.of("contents", Base64.encodeBase64String(contents)));
        }
        // we have an address book update, refresh the local file
        if (isFileAddressBook(fileId)) {
            try {
                networkAddressBook.append(contents);
            } catch (IOException e) {
                throw new ParserException("Error appending to network address book", e);
            }
        }
    }

    private void addFileUpdateFields(Map<String, Object> bqRow, FileUpdateTransactionBody fileUpdate) {
        FileID fileId = fileUpdate.getFileID();
        bqRow.putAll(getCudEntityBq(fileId));
        byte[] contents = fileUpdate.getContents().toByteArray();
        if (parserProperties.getPersist().isFiles() ||
                (parserProperties.getPersist().isSystemFiles() && fileId.getFileNum() < 1000)) {
            getBodyMap(bqRow).put("fileUpdate", Map.of("contents", Base64.encodeBase64String(contents)));
        }
        // we have an address book update, refresh the local file
        if (isFileAddressBook(fileId)) {
            try {
                networkAddressBook.update(contents);
            } catch (IOException e) {
                throw new ParserException("Error appending to network address book", e);
            }
        }
    }

    private void addCryptoCreateAccountFields(Map<String, Object> bqRow, CryptoCreateTransactionBody cryptoCreate) {
        Map<String, Object> fields = new HashMap<>();
        fields.put("initialBalance", cryptoCreate.getInitialBalance());
        if (cryptoCreate.hasProxyAccountID()) {
            fields.put("proxyAccountID", makeEntityMap(cryptoCreate.getProxyAccountID()));
        }
        getBodyMap(bqRow).put("cryptoCreateAccount", fields);
    }

    private void addCryptoAddClaimFields(Map<String, Object> bqRow, CryptoAddClaimTransactionBody cryptoAddClaim) {
        bqRow.putAll(getCudEntityBq(cryptoAddClaim.getClaim().getAccountID()));
        if (parserProperties.getPersist().isClaims()) {
            getBodyMap(bqRow).put("cryptoAddClaim", Map.of("claim",
                    Map.of("hash", Base64.encodeBase64String(cryptoAddClaim.getClaim().getHash().toByteArray()))));
        }
    }

    private void addContractCallFields(
            Map<String, Object> bqRow, ContractCallTransactionBody contractCall) {
        bqRow.putAll(getCudEntityBq(contractCall.getContractID()));
        if (parserProperties.getPersist().isContracts()) {
            Map<String, Object> fields = new HashMap<>();
            fields.put("gas", contractCall.getGas());
            fields.put("functionParameters", Base64
                    .encodeBase64String(contractCall.getFunctionParameters().toByteArray()));
            fields.put("amount", contractCall.getAmount());
            getBodyMap(bqRow).put("contractCall", fields);
        }
    }

    private void addContractCreateFields(
            Map<String, Object> bqRow, ContractCreateTransactionBody contractCreate) {
        if (parserProperties.getPersist().isContracts()) {
            Map<String, Object> fields = new HashMap<>();
            fields.put("gas", contractCreate.getGas());
            fields.put("initialBalance", contractCreate.getInitialBalance());
            fields.put("constructorParameters", Base64
                    .encodeBase64String(contractCreate.getConstructorParameters().toByteArray()));
            fields.put("memo", Base64.encodeBase64String(contractCreate.getMemoBytes().toByteArray()));
            getBodyMap(bqRow).put("contractCreateInstance", fields);
        }
    }

    private static TransactionBody getTransactionBody(Transaction transaction) {
        if (transaction.hasBody()) {
            return transaction.getBody();
        } else {
            try {
                return TransactionBody.parseFrom(transaction.getBodyBytes());
            } catch (InvalidProtocolBufferException e) {
                throw new ParserException("Error parsing transaction from body bytes", e);
            }
        }
    }

    /**
     * Because body.getDataCase() can return null for unknown transaction types, we instead get oneof generically
     *
     * @return The protobuf ID that represents the transaction type
     */
    private static int getTransactionType(TransactionBody body) {
        TransactionBody.DataCase dataCase = body.getDataCase();

        if (dataCase == null || dataCase == TransactionBody.DataCase.DATA_NOT_SET) {
            Set<Integer> unknownFields = body.getUnknownFields().asMap().keySet();

            if (unknownFields.size() != 1) {
                throw new IllegalStateException("Unable to guess correct transaction type since there's not exactly " +
                        "one: " + unknownFields);
            }

            int transactionType = unknownFields.iterator().next();
            log.warn("Encountered unknown transaction type: {}", transactionType);
            return transactionType;
        }

        return dataCase.getNumber();
    }

    private static Map<String, Object> getTransferListFields(Iterator<AccountAmount> accountAmounts) {
        List<Map<String, Object>> accountAmountsList = new ArrayList<>();
        accountAmounts.forEachRemaining(aa -> {
            Map<String, Object> accountAmountFields = new HashMap<>();
            accountAmountFields.put("accountID", makeEntityMap(aa.getAccountID()));
            accountAmountFields.put("amount", aa.getAmount());
            accountAmountsList.add(accountAmountFields);
        });
        return Map.of("accountAmounts", accountAmountsList);
    }

    private static Map<String, Object> getContractFunctionResultFields(ContractFunctionResult contractFunctionResult) {
        Map<String, Object> fields = new HashMap<>();
        fields.put("contractCallResult",
                Base64.encodeBase64String(contractFunctionResult.getContractCallResult().toByteArray()));
        fields.put("errorMessage", contractFunctionResult.getErrorMessage());
        fields.put("gasUsed", contractFunctionResult.getGasUsed());
        return fields;
    }

    public static Map<String, Object> getCudEntityBq(AccountID accountID) {
        if (accountID == AccountID.getDefaultInstance()) {
            return EMPTY_MAP;
        }
        return getCudEntityBq(accountID.getShardNum(), accountID.getRealmNum(), accountID
                .getAccountNum(), EntityType.ACCOUNT);
    }

    public static Map<String, Object> getCudEntityBq(ContractID cid) {
        if (cid == ContractID.getDefaultInstance()) {
            return EMPTY_MAP;
        }
        return getCudEntityBq(cid.getShardNum(), cid.getRealmNum(), cid.getContractNum(), EntityType.CONTRACT);
    }

    public static Map<String, Object> getCudEntityBq(FileID fileId) {
        if (fileId == FileID.getDefaultInstance()) {
            return EMPTY_MAP;
        }
        return getCudEntityBq(fileId.getShardNum(), fileId.getRealmNum(), fileId.getFileNum(), EntityType.FILE);
    }

    public static Map<String, Object> getCudEntityBq(TopicID topicId) {
        if (topicId == TopicID.getDefaultInstance()) {
            return EMPTY_MAP;
        }
        return getCudEntityBq(topicId.getShardNum(), topicId.getRealmNum(), topicId.getTopicNum(), EntityType.TOPIC);
    }

    private static Map<String, Object> getCudEntityBq(long shardNum, long realmNum, long entityNum, EntityType type) {
        Map<String, Object> cudEntityBq = new HashMap<>();
        cudEntityBq.put("cudEntityID", makeEntityMap(shardNum, realmNum, entityNum));
        cudEntityBq.put("cudEntityType", type.ordinal());
        return cudEntityBq;
    }

    public static Map<String, Object> makeEntityMap(AccountID accountID) {
        return makeEntityMap(accountID.getShardNum(), accountID.getRealmNum(), accountID.getAccountNum());
    }

    private static Map<String, Object> getBodyMap(Map<String, Object> bqRow) {
        return ((Map<String, Object>) ((Map<String, Object>) bqRow.get("transaction")).get("body"));
    }

    public static Map<String, Object> makeEntityMap(long shardNum, long realmNum, long entityNum) {
        Map<String, Object> entity = new HashMap<>();
        entity.put("shardNum", shardNum);
        entity.put("realmNum", realmNum);
        entity.put("entityNum", entityNum);
        return entity;
    }

    private static boolean isFileAddressBook(FileID fileId) {
        return (fileId.getFileNum() == 102) && (fileId.getShardNum() == 0) && (fileId.getRealmNum() == 0);
    }

    enum EntityType {
        ZERO, ACCOUNT, CONTRACT, FILE, TOPIC
    }
}
