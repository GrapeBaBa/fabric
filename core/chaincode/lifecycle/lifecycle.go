/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package lifecycle

import (
	"bytes"
	"fmt"

	"github.com/hyperledger/fabric/common/chaincode"
	"github.com/hyperledger/fabric/common/flogging"
	"github.com/hyperledger/fabric/core/chaincode/persistence"
	cb "github.com/hyperledger/fabric/protos/common"
	lb "github.com/hyperledger/fabric/protos/peer/lifecycle"

	"github.com/golang/protobuf/proto"
	"github.com/hyperledger/fabric/core/container/ccintf"
	"github.com/pkg/errors"
)

var logger = flogging.MustGetLogger("lifecycle")

const (
	// NamespacesName is the prefix (or namespace) of the DB which will be used to store
	// the information about other namespaces (for things like chaincodes) in the DB.
	// We want a sub-namespaces within lifecycle in case other information needs to be stored here
	// in the future.
	NamespacesName = "namespaces"

	// ChaincodeSourcesName is the namespace reserved for storing the information about where
	// to find the chaincode (such as as a package on the local filesystem, or in the future,
	// at some network resource).  This namespace is only populated in the org implicit collection.
	ChaincodeSourcesName = "chaincode-sources"

	// ChaincodeDefinitionType is the name of the type used to store defined chaincodes
	ChaincodeDefinitionType = "ChaincodeDefinition"

	// FriendlyChaincodeDefinitionType is the name exposed to the outside world for the chaincode namespace
	FriendlyChaincodeDefinitionType = "Chaincode"
)

// Sequences are the underpinning of the definition framework for lifecycle.  All definitions
// must have a Sequence field in the public state.  This sequence is incremented by exactly 1 with
// each redefinition of the namespace.  The private state org approvals also have a Sequence number
// embedded into the key which matches them either to the vote for the commit, or registers agreement
// with an already committed definition.
//
// Public/World DB layout looks like the following:
// namespaces/metadata/<namespace> -> namespace metadata, including namespace type
// namespaces/fields/<namespace>/Sequence -> sequence for this namespace
// namespaces/fields/<namespace>/<field> -> field of namespace type
//
// So, for instance, a db might look like:
//
// namespaces/metadata/mycc:                   "ChaincodeDefinition"
// namespaces/fields/mycc/Sequence             1 (The current sequence)
// namespaces/fields/mycc/EndorsementInfo:     {Version: "1.3", EndorsementPlugin: "builtin", InitRequired: true}
// namespaces/fields/mycc/ValidationInfo:      {ValidationPlugin: "builtin", ValidationParameter: <application-policy>}
// namespaces/fields/mycc/Collections          {<collection info>}
//
// Private/Org Scope Implcit Collection layout looks like the following
// namespaces/metadata/<namespace>#<sequence_number> -> namespace metadata, including type
// namespaces/fields/<namespace>#<sequence_number>/<field>  -> field of namespace type
//
// namespaces/metadata/mycc#1:                   "ChaincodeParameters"
// namespaces/fields/mycc#1/EndorsementInfo:     {Version: "1.3", EndorsementPlugin: "builtin", InitRequired: true}
// namespaces/fields/mycc#1/ValidationInfo:      {ValidationPlugin: "builtin", ValidationParameter: <application-policy>}
// namespaces/fields/mycc#1/Collections          {<collection info>}
// namespaces/metadata/mycc#2:                   "ChaincodeParameters"
// namespaces/fields/mycc#2/EndorsementInfo:     {Version: "1.4", EndorsementPlugin: "builtin", InitRequired: true}
// namespaces/fields/mycc#2/ValidationInfo:      {ValidationPlugin: "builtin", ValidationParameter: <application-policy>}
// namespaces/fields/mycc#2/Collections          {<collection info>}
//
// chaincode-source/metadata/mycc#1              "LocalPackage"
// chaincode-source/fields/mycc#1/Hash           "hash1"

// ChaincodePackage is a type of chaincode-source which may be serialized into the
// org's private data collection.
// WARNING: This structure is serialized/deserialized from the DB, re-ordering or adding fields
// will cause opaque checks to fail.
type ChaincodeLocalPackage struct {
	Hash []byte
}

// ChaincodeParameters are the parts of the chaincode definition which are serialized
// as values in the statedb.  It is expected that any instance will have no nil fields once initialized.
// WARNING: This structure is serialized/deserialized from the DB, re-ordering or adding fields
// will cause opaque checks to fail.
type ChaincodeParameters struct {
	EndorsementInfo *lb.ChaincodeEndorsementInfo
	ValidationInfo  *lb.ChaincodeValidationInfo
	Collections     *cb.CollectionConfigPackage
}

func (cp *ChaincodeParameters) Equal(ocp *ChaincodeParameters) error {
	switch {
	case cp.EndorsementInfo.Version != ocp.EndorsementInfo.Version:
		return errors.Errorf("Version '%s' != '%s'", cp.EndorsementInfo.Version, ocp.EndorsementInfo.Version)
	case cp.EndorsementInfo.EndorsementPlugin != ocp.EndorsementInfo.EndorsementPlugin:
		return errors.Errorf("EndorsementPlugin '%s' != '%s'", cp.EndorsementInfo.EndorsementPlugin, ocp.EndorsementInfo.EndorsementPlugin)
	case cp.ValidationInfo.ValidationPlugin != ocp.ValidationInfo.ValidationPlugin:
		return errors.Errorf("ValidationPlugin '%s' != '%s'", cp.ValidationInfo.ValidationPlugin, ocp.ValidationInfo.ValidationPlugin)
	case !bytes.Equal(cp.ValidationInfo.ValidationParameter, ocp.ValidationInfo.ValidationParameter):
		return errors.Errorf("ValidationParameter '%x' != '%x'", cp.ValidationInfo.ValidationParameter, ocp.ValidationInfo.ValidationParameter)
	case !proto.Equal(cp.Collections, ocp.Collections):
		return errors.Errorf("Collections do not match")
	default:
	}
	return nil
}

// ChaincodeDefinition contains the chaincode parameters, as well as the sequence number of the definition.
// Note, it does not embed ChaincodeParameters so as not to complicate the serialization.  It is expected
// that any instance will have no nil fields once initialized.
// WARNING: This structure is serialized/deserialized from the DB, re-ordering or adding fields
// will cause opaque checks to fail.
type ChaincodeDefinition struct {
	Sequence        int64
	EndorsementInfo *lb.ChaincodeEndorsementInfo
	ValidationInfo  *lb.ChaincodeValidationInfo
	Collections     *cb.CollectionConfigPackage
}

// Parameters returns the non-sequence info of the chaincode definition
func (cd *ChaincodeDefinition) Parameters() *ChaincodeParameters {
	return &ChaincodeParameters{
		EndorsementInfo: cd.EndorsementInfo,
		ValidationInfo:  cd.ValidationInfo,
		Collections:     cd.Collections,
	}
}

// ChaincodeStore provides a way to persist chaincodes
type ChaincodeStore interface {
	Save(name, version string, ccInstallPkg []byte) (hash []byte, err error)
	// FIXME: this is just a hack to get the green path going; the hash lookup step will disappear in the upcoming CRs
	RetrieveHash(packageID ccintf.CCID) (hash []byte, err error)
	ListInstalledChaincodes() ([]chaincode.InstalledChaincode, error)
	Load(hash []byte) (ccInstallPkg []byte, metadata []*persistence.ChaincodeMetadata, err error)
}

type PackageParser interface {
	Parse(data []byte) (*persistence.ChaincodePackage, error)
}

//go:generate counterfeiter -o mock/install_listener.go --fake-name InstallListener . InstallListener
type InstallListener interface {
	HandleChaincodeInstalled(md *persistence.ChaincodePackageMetadata, hash []byte)
}

// Resources stores the common functions needed by all components of the lifecycle
// by the SCC as well as internally.  It also has some utility methods attached to it
// for querying the lifecycle definitions.
type Resources struct {
	ChannelConfigSource ChannelConfigSource
	ChaincodeStore      ChaincodeStore
	PackageParser       PackageParser
	Serializer          *Serializer
}

// ChaincodeDefinitionIfDefined returns whether the chaincode name is defined in the new lifecycle, a shim around
// the SimpleQueryExecutor to work with the serializer, or an error.  If the namespace is defined, but it is
// not a chaincode, this is considered an error.
func (r *Resources) ChaincodeDefinitionIfDefined(chaincodeName string, state ReadableState) (bool, *ChaincodeDefinition, error) {
	if chaincodeName == LifecycleNamespace {
		return true, &ChaincodeDefinition{
			EndorsementInfo: &lb.ChaincodeEndorsementInfo{
				InitRequired: false,
			},
			ValidationInfo: &lb.ChaincodeValidationInfo{},
		}, nil
	}

	metadata, ok, err := r.Serializer.DeserializeMetadata(NamespacesName, chaincodeName, state)
	if err != nil {
		return false, nil, errors.WithMessage(err, fmt.Sprintf("could not deserialize metadata for chaincode %s", chaincodeName))
	}

	if !ok {
		return false, nil, nil
	}

	if metadata.Datatype != ChaincodeDefinitionType {
		return false, nil, errors.Errorf("not a chaincode type: %s", metadata.Datatype)
	}

	definedChaincode := &ChaincodeDefinition{}
	err = r.Serializer.Deserialize(NamespacesName, chaincodeName, metadata, definedChaincode, state)
	if err != nil {
		return false, nil, errors.WithMessage(err, fmt.Sprintf("could not deserialize chaincode definition for chaincode %s", chaincodeName))
	}

	return true, definedChaincode, nil
}

// ExternalFunctions is intended primarily to support the SCC functions.  In general,
// its methods signatures produce writes (which must be commmitted as part of an endorsement
// flow), or return human readable errors (for instance indicating a chaincode is not found)
// rather than sentinals.  Instead, use the utility functions attached to the lifecycle Resources
// when needed.
type ExternalFunctions struct {
	Resources       *Resources
	InstallListener InstallListener
}

// CommitChaincodeDefinition takes a chaincode definition, checks that its sequence number is the next allowable sequence number,
// checks which organizations agree with the definition, and applies the definition to the public world state.
// It is the responsibility of the caller to check the agreement to determine if the result is valid (typically
// this means checking that the peer's own org is in agreement.)
func (ef *ExternalFunctions) CommitChaincodeDefinition(name string, cd *ChaincodeDefinition, publicState ReadWritableState, orgStates []OpaqueState) ([]bool, error) {
	currentSequence, err := ef.Resources.Serializer.DeserializeFieldAsInt64(NamespacesName, name, "Sequence", publicState)
	if err != nil {
		return nil, errors.WithMessage(err, "could not get current sequence")
	}

	if cd.Sequence != currentSequence+1 {
		return nil, errors.Errorf("requested sequence is %d, but new definition must be sequence %d", cd.Sequence, currentSequence+1)
	}

	agreement := make([]bool, len(orgStates))
	privateName := fmt.Sprintf("%s#%d", name, cd.Sequence)
	for i, orgState := range orgStates {
		match, err := ef.Resources.Serializer.IsSerialized(NamespacesName, privateName, cd.Parameters(), orgState)
		agreement[i] = (err == nil && match)
	}

	if err = ef.Resources.Serializer.Serialize(NamespacesName, name, cd, publicState); err != nil {
		return nil, errors.WithMessage(err, "could not serialize chaincode definition")
	}

	return agreement, nil
}

// ApproveChaincodeDefinitionForOrg adds a chaincode definition entry into the passed in Org state.  The definition must be
// for either the currently defined sequence number or the next sequence number.  If the definition is
// for the current sequence number, then it must match exactly the current definition or it will be rejected.
func (ef *ExternalFunctions) ApproveChaincodeDefinitionForOrg(name string, cd *ChaincodeDefinition, localPackageHash []byte, publicState ReadableState, orgState ReadWritableState) error {
	// Get the current sequence from the public state
	currentSequence, err := ef.Resources.Serializer.DeserializeFieldAsInt64(NamespacesName, name, "Sequence", publicState)
	if err != nil {
		return errors.WithMessage(err, "could not get current sequence")
	}

	requestedSequence := cd.Sequence

	if currentSequence == requestedSequence && requestedSequence == 0 {
		return errors.Errorf("requested sequence is 0, but first definable sequence number is 1")
	}

	if requestedSequence < currentSequence {
		return errors.Errorf("currently defined sequence %d is larger than requested sequence %d", currentSequence, requestedSequence)
	}

	if requestedSequence > currentSequence+1 {
		return errors.Errorf("requested sequence %d is larger than the next available sequence number %d", requestedSequence, currentSequence+1)
	}

	if requestedSequence == currentSequence {
		metadata, ok, err := ef.Resources.Serializer.DeserializeMetadata(NamespacesName, name, publicState)
		if err != nil {
			return errors.WithMessage(err, "could not fetch metadata for current definition")
		}
		if !ok {
			return errors.Errorf("missing metadata for currently committed sequence number (%d)", currentSequence)
		}

		definedChaincode := &ChaincodeDefinition{}
		if err := ef.Resources.Serializer.Deserialize(NamespacesName, name, metadata, definedChaincode, publicState); err != nil {
			return errors.WithMessage(err, fmt.Sprintf("could not deserialize namespace %s as chaincode", name))
		}

		if err := definedChaincode.Parameters().Equal(cd.Parameters()); err != nil {
			return errors.WithMessage(err, "attempted to define the current sequence (%d) for namespace %s, but")
		}
	}

	privateName := fmt.Sprintf("%s#%d", name, requestedSequence)
	if err := ef.Resources.Serializer.Serialize(NamespacesName, privateName, cd.Parameters(), orgState); err != nil {
		return errors.WithMessage(err, "could not serialize chaincode parameters to state")
	}

	if localPackageHash != nil {
		if err := ef.Resources.Serializer.Serialize(ChaincodeSourcesName, privateName, &ChaincodeLocalPackage{
			Hash: localPackageHash,
		}, orgState); err != nil {
			return errors.WithMessage(err, "could not serialize chaincode package info to state")
		}
	}

	return nil
}

// QueryChaincodeDefinition returns the defined chaincode by the given name (if it is defined, and a chaincode)
// or otherwise returns an error.
func (ef *ExternalFunctions) QueryChaincodeDefinition(name string, publicState ReadableState) (*ChaincodeDefinition, error) {
	metadata, ok, err := ef.Resources.Serializer.DeserializeMetadata(NamespacesName, name, publicState)
	if err != nil {
		return nil, errors.WithMessage(err, fmt.Sprintf("could not fetch metadata for namespace %s", name))
	}
	if !ok {
		return nil, errors.Errorf("namespace %s is not defined", name)
	}

	definedChaincode := &ChaincodeDefinition{}
	if err := ef.Resources.Serializer.Deserialize(NamespacesName, name, metadata, definedChaincode, publicState); err != nil {
		return nil, errors.WithMessage(err, fmt.Sprintf("could not deserialize namespace %s as chaincode", name))
	}

	return definedChaincode, nil
}

// InstallChaincode installs a given chaincode to the peer's chaincode store.
// It returns the hash to reference the chaincode by or an error on failure.
func (ef *ExternalFunctions) InstallChaincode(name, version string, chaincodeInstallPackage []byte) ([]byte, error) {
	// Let's validate that the chaincodeInstallPackage is at least well formed before writing it
	pkg, err := ef.Resources.PackageParser.Parse(chaincodeInstallPackage)
	if err != nil {
		return nil, errors.WithMessage(err, "could not parse as a chaincode install package")
	}

	hash, err := ef.Resources.ChaincodeStore.Save(name, version, chaincodeInstallPackage)
	if err != nil {
		return nil, errors.WithMessage(err, "could not save cc install package")
	}

	if ef.InstallListener != nil {
		ef.InstallListener.HandleChaincodeInstalled(pkg.Metadata, hash)
	}

	return hash, nil
}

// QueryNamespaceDefinitions lists the publicly defined namespaces in a channel.  Today it should only ever
// find Datatype encodings of 'ChaincodeDefinition'.  In the future as we support encodings like 'TokenManagementSystem'
// or similar, additional statements will be added to the switch.
func (ef *ExternalFunctions) QueryNamespaceDefinitions(publicState RangeableState) (map[string]string, error) {
	metadatas, err := ef.Resources.Serializer.DeserializeAllMetadata(NamespacesName, publicState)
	if err != nil {
		return nil, errors.WithMessage(err, "could not query namespace metadata")
	}

	result := map[string]string{}
	for key, value := range metadatas {
		switch value.Datatype {
		case ChaincodeDefinitionType:
			result[key] = FriendlyChaincodeDefinitionType
		default:
			// This should never execute, but seems preferable to returning an error
			result[key] = value.Datatype
		}
	}
	return result, nil
}

// QueryInstalledChaincode returns the hash of an installed chaincode of a given name and version.
func (ef *ExternalFunctions) QueryInstalledChaincode(name, version string) ([]byte, error) {
	hash, err := ef.Resources.ChaincodeStore.RetrieveHash(ccintf.CCID(name + ":" + version))
	if err != nil {
		return nil, errors.WithMessage(err, fmt.Sprintf("could not retrieve hash for chaincode '%s:%s'", name, version))
	}

	return hash, nil
}

// QueryInstalledChaincodes returns a list of installed chaincodes
func (ef *ExternalFunctions) QueryInstalledChaincodes() ([]chaincode.InstalledChaincode, error) {
	return ef.Resources.ChaincodeStore.ListInstalledChaincodes()
}
